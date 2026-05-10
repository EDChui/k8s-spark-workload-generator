# Setup

## Environment Explanation

`node5` is the physical machine hosting the VMs and the NFS server.

`cloud_controller_echui` is the VM on `node5` that serves as the control plane for the Kubernetes cluster.

## NFS to share TPC-DS data across VMs

Due to limited storage capacity of each VMs, we setup an NFS server on `node5` to share the TPC-DS data across VMs.

In `node5`:

```bash
mkdir -p /mnt/sdc/echui/tpcds
sudo nano /etc/exports
```

Add the following line, where `192.168.134.0/24` is the IP range of the VMs:

```
/mnt/sdc/echui/tpcds 192.168.134.0/24(rw,sync,no_subtree_check)
```

Then

```bash
sudo exportfs -rav
sudo systemctl restart nfs-kernel-server
sudo mkdir -p /mnt/sdc/echui/tpcds/tpcds-baseline
sudo mkdir -p /mnt/sdc/echui/tpcds/tpcds-baseline/data
sudo mkdir -p /mnt/sdc/echui/tpcds/tpcds-baseline/eventlog
sudo mkdir -p /mnt/sdc/echui/tpcds/tpcds-baseline/tmp
sudo chmod 777 /mnt/sdc/echui/tpcds/tpcds-baseline
sudo chmod 777 /mnt/sdc/echui/tpcds/tpcds-baseline/data
sudo chmod 777 /mnt/sdc/echui/tpcds/tpcds-baseline/eventlog
sudo chmod 777 /mnt/sdc/echui/tpcds/tpcds-baseline/tmp
```

In each of the VMs:

```bash
sudo apt install nfs-common -y
sudo mkdir -p /mnt/tpcds
sudo mount 192.168.1.105:/mnt/sdc/echui/tpcds /mnt/tpcds/

# Write test to verify the NFS mount is working
touch /mnt/tpcds/tpcds-baseline/write-test-$(hostname)
```

## Kubernetes Setup

Capture the Kubernetes API URL:

```bash
export K8S_API="$(kubectl config view --minify -o jsonpath='{.clusters[0].cluster.server}')"
echo "$K8S_API"
```

Create the Spark service account:

```bash
kubectl create serviceaccount spark -n default --dry-run=client -o yaml | kubectl apply -f -

kubectl create clusterrolebinding spark-role \
  --clusterrole=edit \
  --serviceaccount=default:spark \
  --dry-run=client -o yaml | kubectl apply -f -

kubectl auth can-i create pods       --as=system:serviceaccount:default:spark
kubectl auth can-i create services   --as=system:serviceaccount:default:spark
kubectl auth can-i create configmaps --as=system:serviceaccount:default:spark
```

All three `kubectl auth` shall return `yes`.

## Executor Priority

```bash
kubectl apply -f assets/setup/spark_executor_priorityclass.yaml
```

## Install Spark Submit Client

```bash
sudo apt-get update
sudo apt-get install -y openjdk-11-jre-headless curl wget tar

cd ~
wget https://archive.apache.org/dist/spark/spark-3.4.4/spark-3.4.4-bin-hadoop3.tgz
tar xzf spark-3.4.4-bin-hadoop3.tgz
rm spark-3.4.4-bin-hadoop3.tgz

~/spark-3.4.4-bin-hadoop3/bin/spark-submit --version
```

# References

The following sections is not part of the setup, but serves as a reference for how to run the TPC-DS data generation and query execution on Kubernetes.

## Smoke-test the Docker image on Kubernetes

```bash
kubectl delete pod tpcds-image-smoke --ignore-not-found=true

kubectl run tpcds-image-smoke \
  --image=elliesgood00/tpcds-image:v17 \
  --restart=Never \
  --command -- sleep 3600

kubectl wait --for=condition=Ready pod/tpcds-image-smoke --timeout=180s

kubectl exec tpcds-image-smoke -- ls -lh /opt/tpcds
kubectl exec tpcds-image-smoke -- ls -lh /opt/tpcds/lib
kubectl exec tpcds-image-smoke -- ls -lh /opt/tpcds-kit/tools/dsdgen

kubectl delete pod tpcds-image-smoke --wait=false
```

You should see at least:

```bash
/opt/tpcds/parquet-data-generator_2.12-1.0.jar
/opt/tpcds/lib/spark-sql-perf_2.12-0.5.1-SNAPSHOT.jar
/opt/tpcds-kit/tools/dsdgen
```

## Define common Spark submit settings

```bash
export SPARK_HOME="$HOME/spark-3.4.4-bin-hadoop3"
export IMAGE="elliesgood00/tpcds-image:v17"
export HOST_TPCDS_DIR="/mnt/tpcds/tpcds-baseline"
export CONTAINER_TPCDS_DIR="/tpcds-data"
export SCALE="1"
export QUERY="q3-v2.4"
```

```bash
spark_tpcds_submit() {
  "$SPARK_HOME/bin/spark-submit" \
    --class ParquetGenerator \
    --master "k8s://$K8S_API" \
    --deploy-mode cluster \
    --conf "spark.kubernetes.container.image=$IMAGE" \
    --conf "spark.kubernetes.container.image.pullPolicy=IfNotPresent" \
    --conf "spark.kubernetes.authenticate.driver.serviceAccountName=spark" \
    --conf "spark.kubernetes.namespace=default" \
    --conf "spark.executor.instances=1" \
    --conf "spark.executor.cores=1" \
    --conf "spark.kubernetes.executor.request.cores=1" \
    --conf "spark.kubernetes.driver.request.cores=1" \
    --conf "spark.executor.memory=2g" \
    --conf "spark.driver.memory=2g" \
    --conf "spark.executor.memoryOverhead=512m" \
    --conf "spark.driver.memoryOverhead=512m" \
    --conf "spark.sql.catalogImplementation=hive" \
    --conf "spark.hadoop.javax.jdo.option.ConnectionURL=jdbc:derby:;databaseName=$CONTAINER_TPCDS_DIR/metastore_db;create=true" \
    --conf "spark.hadoop.javax.jdo.option.ConnectionDriverName=org.apache.derby.jdbc.EmbeddedDriver" \
    --conf "spark.sql.warehouse.dir=$CONTAINER_TPCDS_DIR/hive-warehouse" \
    --conf "spark.eventLog.enabled=true" \
    --conf "spark.eventLog.dir=file://$CONTAINER_TPCDS_DIR/eventlog" \
    --conf "spark.local.dir=$CONTAINER_TPCDS_DIR/tmp" \
    --conf "spark.kubernetes.driver.volumes.hostPath.tpcds.mount.path=$CONTAINER_TPCDS_DIR" \
    --conf "spark.kubernetes.driver.volumes.hostPath.tpcds.mount.readOnly=false" \
    --conf "spark.kubernetes.driver.volumes.hostPath.tpcds.options.path=$HOST_TPCDS_DIR" \
    --conf "spark.kubernetes.driver.volumes.hostPath.tpcds.options.type=Directory" \
    --conf "spark.kubernetes.executor.volumes.hostPath.tpcds.mount.path=$CONTAINER_TPCDS_DIR" \
    --conf "spark.kubernetes.executor.volumes.hostPath.tpcds.mount.readOnly=false" \
    --conf "spark.kubernetes.executor.volumes.hostPath.tpcds.options.path=$HOST_TPCDS_DIR" \
    --conf "spark.kubernetes.executor.volumes.hostPath.tpcds.options.type=Directory" \
    --jars "local:///opt/tpcds/lib/spark-sql-perf_2.12-0.5.1-SNAPSHOT.jar" \
    "local:///opt/tpcds/parquet-data-generator_2.12-1.0.jar" \
    "$@"
}
```

## Run TPC-DS data generation and query execution on Kubernetes

```bash
spark_tpcds_submit datagen /tpcds-data /opt/tpcds-kit/tools "$SCALE"
kubectl get pods --sort-by=.metadata.creationTimestamp | tail -5

spark_tpcds_submit metagen /tpcds-data "$SCALE"
kubectl get pods --sort-by=.metadata.creationTimestamp | tail -5

spark_tpcds_submit query "$QUERY" "$SCALE" 1
kubectl get pods --sort-by=.metadata.creationTimestamp | tail -5

DRIVER="$(kubectl get pods --sort-by=.metadata.creationTimestamp -o name | grep driver | tail -1 | cut -d/ -f2)"
kubectl logs "$DRIVER" | tee /mnt/tpcds/tpcds-baseline/q3-v2.4.log
kubectl get pod "$DRIVER"
```
