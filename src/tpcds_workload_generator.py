import logging
import random
import time
import yaml
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from kubernetes import client, config
from kubernetes.client.rest import ApiException
from pathlib import Path
from typing import List, Optional

from spark_submit_runner import SparkSubmitRunner, SparkSubmitConfig

logger = logging.getLogger(__name__)


@dataclass
class WorkloadStage:
    amount: int
    iat_seconds: int


@dataclass
class TPCDSGeneratorConfig:
    seed: int
    delete_after_seconds: int
    status_poll_seconds: int
    scale: int
    queries: List[str]
    workloads: List[WorkloadStage]
    metastore_dirs: Optional[List[str]] = None


def load_generator_config(config_path: Path) -> TPCDSGeneratorConfig:
    with open(config_path, "r") as f:
        config_data = yaml.safe_load(f)

    workloads = [WorkloadStage(**stage) for stage in config_data["workloads"]]
    return TPCDSGeneratorConfig(
        seed=config_data["seed"],
        delete_after_seconds=config_data["delete_after_seconds"],
        status_poll_seconds=config_data["status_poll_seconds"],
        scale=config_data["scale"],
        queries=config_data["queries"],
        workloads=workloads,
        metastore_dirs=config_data.get("metastore_dirs"),
    )


class TPCDSWorkloadGenerator:
    def __init__(self, runner: SparkSubmitRunner):
        self.runner = runner
        
        config.load_kube_config()
        self.k8s_client = client.CoreV1Api()

    def _get_pod_completed_at(self, pod) -> Optional[datetime]:
        finished_times = []
        for container_status in pod.status.container_statuses or []:
            state = container_status.state
            terminated = state.terminated if state else None
            if terminated and terminated.finished_at:
                finished_times.append(terminated.finished_at)

        if finished_times:
            return max(finished_times)

        return None

    def _check_and_delete_completed_driver_pods(self, namespace: str, delete_after_seconds: int) -> int:
        now = datetime.now(timezone.utc)
        running_driver_pods = 0

        pods = self.k8s_client.list_namespaced_pod(namespace=namespace).items
        for pod in pods:
            pod_name: str = pod.metadata.name
            phase: str = pod.status.phase

            # Simply identify Spark driver pods by name pattern
            if not pod_name.endswith("-driver"):
                continue

            if phase not in ("Succeeded", "Failed"):
                running_driver_pods += 1
                continue

            completed_at = self._get_pod_completed_at(pod)
            if completed_at is None:
                running_driver_pods += 1
                logger.warning(f"Driver pod {pod_name} is {phase}, but completion time is unavailable; skipping deletion")
                continue

            age_seconds = (now - completed_at).total_seconds()
            if age_seconds < delete_after_seconds:
                running_driver_pods += 1
                continue

            try:
                if phase == "Failed":
                    logger.warning(f"Driver pod {pod_name} failed {age_seconds:.1f} seconds ago; deleting it")
                self.k8s_client.delete_namespaced_pod(name=pod_name, namespace=namespace)
                logger.debug(f"Deleted completed driver pod {pod_name} (age: {age_seconds:.1f} seconds)")
            except ApiException as e:
                if e.status == 404:
                    logger.warning(f"Driver pod {pod_name} already deleted by another process")
                else:
                    logger.error(f"Failed to delete driver pod {pod_name}: {e}")

        return running_driver_pods
    
    def _wait_with_cleanup(self, namespace: str, wait_seconds: float, delate_after_seconds: int, status_poll_seconds: int):
        end_time = time.monotonic() + wait_seconds
        while True:
            self._check_and_delete_completed_driver_pods(namespace, delate_after_seconds)
            remaining_seconds = end_time - time.monotonic()
            if remaining_seconds <= 0:
                return
            sleep_seconds = min(status_poll_seconds, remaining_seconds)
            time.sleep(sleep_seconds)

    def _drain_remaining_pods(self, namespace: str, delete_after_seconds: int, status_poll_seconds: int):
        while True:
            remaining_pods = self._check_and_delete_completed_driver_pods(namespace=namespace, delete_after_seconds=delete_after_seconds)
            if remaining_pods == 0:
                logger.info("All driver pods have completed and been cleaned up")
                return
            logger.info(f"Waiting for {remaining_pods} remaining driver pods to complete and be cleaned up")
            time.sleep(status_poll_seconds)
    
    def run_poisson(self, spark_submit_config: SparkSubmitConfig, generator_config: TPCDSGeneratorConfig):
        rng = random.Random(generator_config.seed)
        metastore_dirs = generator_config.metastore_dirs or [None]
        total_launch_count = 0

        for stage_idx, stage in enumerate(generator_config.workloads):
            logger.info(f"Starting workload stage {stage_idx + 1}/{len(generator_config.workloads)}: {stage.amount} queries with mean IAT {stage.iat_seconds} seconds")
            for launch_idx in range(stage.amount):
                query = rng.choice(generator_config.queries)
                
                # Select a metastore_dir for this launch to solve the error "ERROR XSDB6: Another instance of Derby may have already booted the database /tpcds-data/metastore_db"
                # TODO: Remove this workaround
                metastore_dir = metastore_dirs[total_launch_count % len(metastore_dirs)]
                if metastore_dir is None:
                    logger.debug("No metastore_dir configured for this stage, using default Spark configuration")
                else:
                    spark_submit_config = replace(spark_submit_config, metastore_dir=metastore_dir)

                # Launch the Spark job for the selected query
                self.runner.run_query(spark_submit_config, generator_config.scale, query, repeat=1)
                total_launch_count += 1
                logger.info(f"Launched {launch_idx + 1}/{stage.amount} of stage {stage_idx + 1}: query={query}")

                self._check_and_delete_completed_driver_pods(
                    namespace=spark_submit_config.namespace,
                    delete_after_seconds=generator_config.delete_after_seconds,
                )
                
                if launch_idx < stage.amount - 1:
                    inter_arrival_seconds = rng.expovariate(1.0 / stage.iat_seconds)
                    logger.info(f"Waiting {inter_arrival_seconds:.1f} seconds before launching next query")
                    self._wait_with_cleanup(
                        namespace=spark_submit_config.namespace,
                        wait_seconds=inter_arrival_seconds,
                        delate_after_seconds=generator_config.delete_after_seconds,
                        status_poll_seconds=generator_config.status_poll_seconds,
                    )

        self._drain_remaining_pods(
            namespace=spark_submit_config.namespace,
            delete_after_seconds=generator_config.delete_after_seconds,
            status_poll_seconds=generator_config.status_poll_seconds,
        )
