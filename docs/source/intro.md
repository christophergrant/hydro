(intro/get-started)=
# Get Started

This page describes how to get started with the hydro, with a focus on installation.

## Standalone Installation

[![PyPI][pypi-badge]][pypi-link]

To install use [pip](https://pip.pypa.io):

```bash
pip install spark-hydro
```

To start an interative shell:

```bash
pyspark --packages io.delta:delta-core_2.12:2.2.0 --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
```

For other methods, follow [this guide](https://docs.delta.io/latest/quick-start.html#set-up-apache-spark-with-delta-lake).

## Databricks

[![PyPI][pypi-badge]][pypi-link]

To install hydro on a Databricks cluster, follow [these directions](https://docs.databricks.com/libraries/cluster-libraries.html#install-a-library-on-a-cluster).

[pypi-badge]: https://img.shields.io/pypi/v/spark-hydro.svg
[pypi-link]: https://pypi.org/project/spark-hydro/
