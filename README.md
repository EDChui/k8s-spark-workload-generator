# K8S Spark Workload Generator

Simple workload generator for Spark on Kubernetes, using TPC-DS queries.

# Requirements

- Python 3.8+

# Setup

Refer to the setup instructions in the [`setup.md`](/docs/setup.md#setup) file for installing dependencies and configuring the environment.

# Usages

Single command to generate data, run metadata generation, and execute a TPC-DS query:

```bash
python3 src/cli.py datagen --config assets/spark_submit_config.yaml --scale 1
python3 src/cli.py metagen --config assets/spark_submit_config.yaml --scale 1
python3 src/cli.py query --config assets/spark_submit_config.yaml --scale 1 --query q3-v2.4
```

Command to run the TPC-DS data generator:

```bash
python3 src/cli.py poisson --spark-config assets/spark_submit_config.yaml --generator-config assets/generator_config.yaml
```

# Keywords

- Spark on Kubernetes
- TPC-DS
- Workload Generator
- Poisson process with inter-arrival times (IAT)

# Credits

This repository is based on the work of [Zhuoran Song](https://github.com/Ellies1/thesis01). We uses their TPC-DS data generator and query docker image [`elliesgood00/tpcds-image:v17`](https://hub.docker.com/r/elliesgood00/tpcds-image) as the basis for our workload generator.
