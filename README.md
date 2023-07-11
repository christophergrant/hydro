# hydro 💧

[![build](https://github.com/christophergrant/hydro/actions/workflows/push.yml/badge.svg?branch=main)](https://github.com/christophergrant/hydro/actions/workflows/push.yml)
[![codecov](https://codecov.io/gh/christophergrant/hydro/branch/main/graph/badge.svg?token=Z64814CV1E)](https://codecov.io/gh/christophergrant/hydro)
![pypidownloads](https://img.shields.io/pypi/dm/spark-hydro?color=%23123d0d&label=pypi%20downloads&style=flat-square)

hydro is a collection of Python-based [Apache Spark](https://spark.apache.org/) and [Delta Lake](https://delta.io/) extensions.

See [Key Functionality](#key-functionality-) for concrete use cases.

## Who are hydro's intended users?

hydro is intended to be used by developers and engineers who interact with Delta Lake tables and Spark DataFrames with Python. It can be used by those of all skill levels.

hydro is compatible with the Databricks platform as well as on other platforms where PySpark and Delta Lake can be installed: laptops for example.

## Warning ⚠️

hydro is well tested but not battle hardened, yet. Use it at your own risk.

## Installation

```commandline
pip install spark-hydro
```

## Docs 📖

https://christophergrant.github.io/hydro

## Key Functionality 🔑

- Correctly perform [Slowly Changing Dimensions (SCD)](https://en.wikipedia.org/wiki/Slowly_changing_dimension) on Delta Lake tables - [hydro.delta.scd](https://christophergrant.github.io/hydro/api/delta.html#hydro.delta.scd) and [hydro.delta.bootstrap_scd2](https://christophergrant.github.io/hydro/delta.html#hydro.delta.bootstrap_scd2)
- Issue queries against Delta Log metadata, quickly and efficiently retrieving file-level metadata even on Petabyte-scale tables - [hydro.delta.file_stats](https://christophergrant.github.io/hydro/api/delta.html#hydro.delta.file_stats), [hydro.delta.partition_stats](https://christophergrant.github.io/hydro/api/delta.html#hydro.delta.partition_stats)
- Infer the schema of JSON columns - [hydro.spark.infer_json_schema](https://christophergrant.github.io/hydro/api/delta.html#hydro.delta.infer_json_field)
- Drop nested fields from a Spark DataFrame [hydro.spark.drop_fields](https://christophergrant.github.io/hydro/api/spark.html#hydro.spark.drop_fields)
- Quality of life improvements like [hydro.delta.detail_enhanced](https://christophergrant.github.io/hydro/api/delta.html#hydro.delta.detail_enhanced) and [hydro.spark.fields](https://christophergrant.github.io/hydro/api/spark.html#hydro.spark.fields)
- And more... check the docs!

## Contributions ✨

Contributions are welcome!

Please [create an issue](https://github.com/christophergrant/hydro/issues/new/choose) and discuss before starting work on a feature to make sure that it aligns with the future of the project.
