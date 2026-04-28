import click
import shutil
import logging
from pathlib import Path

from spark_submit_runner import SparkSubmitRunner, load_spark_submit_config
from tpcds_workload_generator import TPCDSWorkloadGenerator, load_generator_config

DEFAULT_SPARK_PREPARE_CONFIG_PATH = "assets/spark_submit_prepare_config.yaml"
DEFAULT_SPARK_CONFIG_PATH = "assets/spark_submit_config.yaml"
DEFAULT_GENERATOR_CONFIG_PATH = "assets/generator_config.yaml"
DEFAULT_SPARK_HOME = "/home/cloud_controller_echui/spark-3.4.4-bin-hadoop3"


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S%z",
)
logger = logging.getLogger(__name__)


@click.group()
def cli():
    pass


@cli.command()
@click.option("--config", required=True, default=DEFAULT_SPARK_PREPARE_CONFIG_PATH, help="Path to the YAML configuration file")
@click.option("--spark-home", required=True, default=DEFAULT_SPARK_HOME, help="Path to the Spark installation directory")
@click.option("--scale", required=True, default=1, help="Scale factor for TPC-DS data")
def datagen(config: str, spark_home: str, scale: int) -> int:
    config_path = Path(config)
    if not config_path.exists():
        click.echo(f"Configuration file not found at {config_path}")
        return 1
    
    spark_submit_config = load_spark_submit_config(config_path)
    spark_submit_runner = SparkSubmitRunner(spark_home=Path(spark_home))
    
    spark_submit_runner.run_datagen(spark_submit_config, scale)
    click.echo("Data generation submitted successfully.")
    return 0


@cli.command()
@click.option("--config", required=True, default=DEFAULT_SPARK_PREPARE_CONFIG_PATH, help="Path to the YAML configuration file")
@click.option("--spark-home", required=True, default=DEFAULT_SPARK_HOME, help="Path to the Spark installation directory")
@click.option("--scale", required=True, default=1, help="Scale factor for TPC-DS data")
def metagen(config: str, spark_home: str, scale: int) -> int:
    config_path = Path(config)
    if not config_path.exists():
        click.echo(f"Configuration file not found at {config_path}")
        return 1
    
    spark_submit_config = load_spark_submit_config(config_path)
    spark_submit_runner = SparkSubmitRunner(spark_home=Path(spark_home))
    
    spark_submit_runner.run_metagen(spark_submit_config, scale)
    click.echo("Metadata generation submitted successfully.")
    return 0


@cli.command()
@click.option("--origin-metastore", required=True, default="/mnt/tpcds/tpcds-baseline/metastore_db", help="Path to the original metastore_db directory to copy from")
@click.option("--amount", required=True, type=click.INT, help="Number of copies to create")
def prepare_metastores(origin_metastore: str, amount: int) -> int:
    origin_path = Path(origin_metastore)
    if not origin_path.exists():
        click.echo(f"Original metastore_db directory not found at {origin_path}")
        return 1
    
    for i in range(amount):
        dest_path = origin_path.parent / f"metastore_db_{i+1}"
        if dest_path.exists():
            click.echo(f"Destination path {dest_path} already exists, skipping copy")
            continue
        
        try:
            shutil.copytree(origin_path, dest_path)
            click.echo(f"Copied {origin_path} to {dest_path}")
        except Exception as e:
            click.echo(f"Error copying to {dest_path}: {e}")
            return 1
    
    click.echo("Metastore preparation completed successfully.")
    return 0


@cli.command()
@click.option("--config", required=True, default=DEFAULT_SPARK_CONFIG_PATH, help="Path to the YAML configuration file")
@click.option("--spark-home", required=True, default=DEFAULT_SPARK_HOME, help="Path to the Spark installation directory")
@click.option("--scale", required=True, default=1,help="Scale factor for TPC-DS data")
@click.option("--query", required=True, default="q3-v2.4", help="TPC-DS query to run, e.g., q3-v2.4")
@click.option("--repeat", required=False, default=1, help="Number of times to repeat the query execution")
def query(config: str, spark_home: str, scale: int, query: str, repeat: int) -> int:
    config_path = Path(config)
    if not config_path.exists():
        click.echo(f"Configuration file not found at {config_path}")
        return 1
    
    spark_submit_config = load_spark_submit_config(config_path)
    spark_submit_runner = SparkSubmitRunner(spark_home=Path(spark_home))
    
    spark_submit_runner.run_query(spark_submit_config, scale, query, repeat)
    click.echo("Query execution submitted successfully.")
    return 0


@cli.command()
@click.option("--spark-config", required=True, default=DEFAULT_SPARK_CONFIG_PATH, help="Path to the Spark submit YAML configuration file")
@click.option("--generator-config", required=True, default=DEFAULT_GENERATOR_CONFIG_PATH, help="Path to the TPC-DS workload generator YAML configuration file")
@click.option("--spark-home", required=True, default=DEFAULT_SPARK_HOME, help="Path to the Spark installation directory")
def poisson(spark_config: str, generator_config: str, spark_home: str) -> int:
    spark_config_path = Path(spark_config)
    generator_config_path = Path(generator_config)

    if not spark_config_path.exists():
        click.echo(f"Spark configuration file not found at {spark_config_path}")
        return 1
    if not generator_config_path.exists():
        click.echo(f"Generator configuration file not found at {generator_config_path}")
        return 1
    
    spark_submit_config = load_spark_submit_config(spark_config_path)
    generator_config = load_generator_config(generator_config_path)
    
    spark_submit_runner = SparkSubmitRunner(spark_home=Path(spark_home))
    workload_generator = TPCDSWorkloadGenerator(runner=spark_submit_runner)
    
    workload_generator.run_poisson(spark_submit_config, generator_config)
    click.echo("TPC-DS workload generation completed successfully.")
    return 0


if __name__ == "__main__":
    cli()