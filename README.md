# K8S Spark Workload Generator

Simple workload generator for Spark on Kubernetes, using TPC-DS queries.

# Requirements

- Python 3.8+

# Setup

Refer to the setup instructions in the [`setup.md`](/docs/setup.md#setup) file for installing dependencies and configuring the environment.

# Usages

## Single Run

Single command to generate data, run metadata generation, and execute a TPC-DS query:

```bash
python3 src/cli.py datagen --config assets/spark_submit_prepare_config.yaml --scale 1
python3 src/cli.py metagen --config assets/spark_submit_prepare_config.yaml --scale 1
python3 src/cli.py query --config assets/spark_submit_config.yaml --scale 1 --query q3-v2.4
```

## Multiple Runs with Poisson Process

Since the original TPC-DS data generator does not support concurrent queries that trying to use the same embedded Derby Hive metastore at the same time (ERROR XSDB6), here is a simple workaround by preparing multiple copies of the `metastore_db` directory and configure each Spark job to use a different copy:

```bash
python3 src/cli.py prepare-metastores --origin-metastore /mnt/tpcds/tpcds-baseline/metastore_db --amount 1
```

Note you have to make sure the generated metastore copies have the same access permission as the original one, in the NFS host, do

```bash
sudo chown -R --reference=metastore_db metastore_db_1
```

Command to run the TPC-DS data generator:

```bash
python3 src/cli.py poisson --generator-config assets/generator_config.yaml
```

# Keywords

- Spark on Kubernetes
- TPC-DS
- Workload Generator
- Poisson process with inter-arrival times (IAT)

# Credits

This repository is based on the work of [Zhuoran Song](https://github.com/Ellies1/thesis01). We uses their TPC-DS data generator and query docker image [`elliesgood00/tpcds-image:v17`](https://hub.docker.com/r/elliesgood00/tpcds-image) as the basis for our workload generator.
