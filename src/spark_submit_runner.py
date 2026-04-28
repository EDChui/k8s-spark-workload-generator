import logging
import os
import subprocess
import yaml
from dataclasses import dataclass
from pathlib import Path
from typing import List

logger = logging.getLogger(__name__)


@dataclass
class SparkDriverConfig:
    cores: int                  = 1
    k8s_request_cores: int      = 1
    k8s_limit_cores: int        = 1
    memory: str                 = "2g"
    memory_overhead: str        = "512m"


@dataclass
class SparkExecutorConfig:
    instances: int              = 1
    cores: int                  = 1
    k8s_request_cores: int      = 1
    k8s_limit_cores: int        = 1
    memory: str                 = "2g"
    memory_overhead: str        = "512m"
    delete_on_termination: bool = True


@dataclass
class SparkSubmitConfig:
    k8s_api: str
    service_account: str        = "spark"
    namespace: str              = "default"
    image: str                  = "elliesgood00/tpcds-image:v17"
    class_name: str             = "ParquetGenerator"
    host_dir: str               = "/mnt/tpcds/tpcds-baseline"
    container_dir: str          = "/tpcds-data"
    wait_app_completion: bool   = False
    driver_config: SparkDriverConfig = SparkDriverConfig()
    executor_config: SparkExecutorConfig = SparkExecutorConfig()


def load_spark_submit_config(config_path: Path) -> SparkSubmitConfig:
    with open(config_path, 'r') as f:
        config_dict = yaml.safe_load(f)
    
    driver_config = SparkDriverConfig(**config_dict.get("driver_config", {}))
    executor_config = SparkExecutorConfig(**config_dict.get("executor_config", {}))
    
    return SparkSubmitConfig(
        k8s_api=config_dict["k8s_api"],
        service_account=config_dict.get("service_account", "spark"),
        namespace=config_dict.get("namespace", "default"),
        image=config_dict.get("image", "elliesgood00/tpcds-image:v17"),
        class_name=config_dict.get("class_name", "ParquetGenerator"),
        host_dir=config_dict.get("host_dir", "/mnt/tpcds/tpcds-baseline"),
        container_dir=config_dict.get("container_dir", "/tpcds-data"),
        wait_app_completion=config_dict.get("wait_app_completion", False),
        driver_config=driver_config,
        executor_config=executor_config
    )


class SparkSubmitRunner:
    def __init__(self, spark_home: Path):
        self.spark_home = spark_home
        self.spark_submit_path = self.spark_home / "bin" / "spark-submit"

        # Verify the binary exists
        if not self.spark_submit_path.exists():
            raise FileNotFoundError(f"spark-submit not found at {self.spark_submit_path}")
        
        # Ensure it is executable
        if not os.access(self.spark_submit_path, os.X_OK):
            raise PermissionError(f"spark-submit at {self.spark_submit_path} is not executable")
        
        logger.info(f"Initialized SparkSubmitRunner with spark-submit at {self.spark_submit_path}")

    def execute_spark_submit(self, args: List[str]) -> subprocess.CompletedProcess:
        env = os.environ.copy()

        cmd = [str(self.spark_submit_path)] + args
        logger.debug(f"Running command: {' '.join(cmd)}")

        try:
            result = subprocess.run(cmd, capture_output=True, text=True, check=True, env=env)
        except subprocess.CalledProcessError as e:
            logger.error(f"Command failed with error: {e.stderr}")
            raise e

        logger.debug(f"Exit code: {result.returncode}")
        if result.stdout:
            logger.debug(f"stdout: {result.stdout}")
        if result.stderr:
            logger.debug(f"stderr: {result.stderr}")

        return result
    
    def _get_k8s_master(self, config: SparkSubmitConfig) -> str:
        api = config.k8s_api
        return api if api.startswith("k8s://") else f"k8s://{api}"
    
    def _add_conf(self, args: List[str], key: str, value: str):
        args.extend(["--conf", f"{key}={value}"])
    
    def build_base_spark_submit_args(self, config: SparkSubmitConfig) -> List[str]:
        args = [
            "--class", config.class_name,
            "--master", self._get_k8s_master(config),
            "--deploy-mode", "cluster"
        ]

        # Spark on K8s settings
        self._add_conf(args, "spark.kubernetes.container.image", config.image)
        self._add_conf(args, "spark.kubernetes.container.image.pullPolicy", "IfNotPresent")
        self._add_conf(args, "spark.kubernetes.authenticate.driver.serviceAccountName", config.service_account)
        self._add_conf(args, "spark.kubernetes.namespace", config.namespace)
        self._add_conf(args, "spark.kubernetes.submission.waitAppCompletion", str(config.wait_app_completion).lower())

        # Driver settings
        self._add_conf(args, "spark.driver.cores", str(config.driver_config.cores))
        self._add_conf(args, "spark.kubernetes.driver.request.cores", str(config.driver_config.k8s_request_cores))
        self._add_conf(args, "spark.kubernetes.driver.limit.cores", str(config.driver_config.k8s_limit_cores))
        self._add_conf(args, "spark.driver.memory", config.driver_config.memory)
        self._add_conf(args, "spark.driver.memoryOverhead", config.driver_config.memory_overhead)

        # Executor settings
        self._add_conf(args, "spark.executor.instances", str(config.executor_config.instances))
        self._add_conf(args, "spark.executor.cores", str(config.executor_config.cores))
        self._add_conf(args, "spark.kubernetes.executor.deleteOnTermination", str(config.executor_config.delete_on_termination).lower())
        self._add_conf(args, "spark.kubernetes.executor.request.cores", str(config.executor_config.k8s_request_cores))
        self._add_conf(args, "spark.kubernetes.executor.limit.cores", str(config.executor_config.k8s_limit_cores))
        self._add_conf(args, "spark.executor.memory", config.executor_config.memory)
        self._add_conf(args, "spark.executor.memoryOverhead", config.executor_config.memory_overhead)

        self._add_conf(args, "spark.sql.catalogImplementation", "hive")
        self._add_conf(args, "spark.hadoop.javax.jdo.option.ConnectionURL", f"jdbc:derby:;databaseName={config.container_dir}/metastore_db;create=true")
        self._add_conf(args, "spark.hadoop.javax.jdo.option.ConnectionDriverName", "org.apache.derby.jdbc.EmbeddedDriver")
        self._add_conf(args, "spark.sql.warehouse.dir", f"{config.container_dir}/hive-warehouse")
        self._add_conf(args, "spark.eventLog.enabled", "true")
        self._add_conf(args, "spark.eventLog.dir", f"file://{config.container_dir}/eventlog")
        self._add_conf(args, "spark.local.dir", f"{config.container_dir}/tmp")

        self._add_conf(args, "spark.kubernetes.driver.volumes.hostPath.tpcds.mount.path", config.container_dir)
        self._add_conf(args, "spark.kubernetes.driver.volumes.hostPath.tpcds.mount.readOnly", "false")
        self._add_conf(args, "spark.kubernetes.driver.volumes.hostPath.tpcds.options.path", config.host_dir)
        self._add_conf(args, "spark.kubernetes.driver.volumes.hostPath.tpcds.options.type", "Directory")
        self._add_conf(args, "spark.kubernetes.executor.volumes.hostPath.tpcds.mount.path", config.container_dir)
        self._add_conf(args, "spark.kubernetes.executor.volumes.hostPath.tpcds.mount.readOnly", "false")
        self._add_conf(args, "spark.kubernetes.executor.volumes.hostPath.tpcds.options.path", config.host_dir)
        self._add_conf(args, "spark.kubernetes.executor.volumes.hostPath.tpcds.options.type", "Directory")

        args.extend([
            "--jars", "local:///opt/tpcds/lib/spark-sql-perf_2.12-0.5.1-SNAPSHOT.jar",
            "local:///opt/tpcds/parquet-data-generator_2.12-1.0.jar"
        ])
        
        return args
    
    def _build_datagen_args(self, config: SparkSubmitConfig, scale: int) -> List[str]:
        args = self.build_base_spark_submit_args(config)
        args.extend([
            "datagen",
            config.container_dir,
            "/opt/tpcds-kit/tools",
            str(scale)
        ])
        return args
    
    def _build_metagen_args(self, config: SparkSubmitConfig, scale: int) -> List[str]:
        args = self.build_base_spark_submit_args(config)
        args.extend([
            "metagen",
            config.container_dir,
            str(scale)
        ])
        return args

    def _build_query_args(self, config: SparkSubmitConfig, scale: int, query: str, repeat: int=1) -> List[str]:
        args = self.build_base_spark_submit_args(config)
        args.extend([
            "query",
            query,
            str(scale),
            str(repeat)
        ])
        return args
    
    def _create_done_file(self, host_dir: str, action: str, scale: int):
        done_file = Path(host_dir) / f"{action}-done-{scale}"
        done_file.touch()
        logger.info(f"Created done file: {done_file}")

    def _is_done_file_exists(self, host_dir: str, action: str, scale: int) -> bool:
        done_file = Path(host_dir) / f"{action}-done-{scale}"
        exists = done_file.exists()
        return exists

    def run_datagen(self, config: SparkSubmitConfig, scale: int) -> subprocess.CompletedProcess:
        args = self._build_datagen_args(config, scale)
        result = self.execute_spark_submit(args)
        if result.returncode == 0:
            self._create_done_file(config.host_dir, "datagen", scale)
        return result
    
    def run_metagen(self, config: SparkSubmitConfig, scale: int) -> subprocess.CompletedProcess:
        if not self._is_done_file_exists(config.host_dir, "datagen", scale):
            raise RuntimeError(f"Data generation not completed for scale {scale}. Please run datagen first.")
        args = self._build_metagen_args(config, scale)
        result = self.execute_spark_submit(args)
        if result.returncode == 0:
            self._create_done_file(config.host_dir, "metagen", scale)
        return result
    
    def run_query(self, config: SparkSubmitConfig, scale: int, query: str, repeat: int=1) -> subprocess.CompletedProcess:
        if not self._is_done_file_exists(config.host_dir, "datagen", scale):
            raise RuntimeError(f"Data generation not completed for scale {scale}. Please run datagen first.")
        if not self._is_done_file_exists(config.host_dir, "metagen", scale):
            raise RuntimeError(f"Metadata generation not completed for scale {scale}. Please run metagen first.")
        args = self._build_query_args(config, scale, query, repeat)
        result = self.execute_spark_submit(args)
        return result
