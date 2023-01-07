# hydro 💧

hydro is a collection of Python-based [Apache Spark](https://spark.apache.org/) and [Delta Lake](https://delta.io/) related tooling.

## Installation

```commandline
pip install delta-hydro
```

## Docs 📖

https://christophergrant.github.io/delta-hydro

## Key Functionality 🔑

- Deduplicate a Delta Lake table, in-place, without a full overwrite - [hydro.delta.deduplicate](https://christophergrant.github.io/delta-hydro/delta.html#hydro.delta.deduplicate)
- Correctly perform [Slowly Changing Dimensions (SCD)](https://en.wikipedia.org/wiki/Slowly_changing_dimension) (types 1 or 2) on Delta Lake tables - [hydro.delta.scd](https://christophergrant.github.io/delta-hydro/delta.html#hydro.delta.scd) and [hydro.delta.bootstrap_scd2](https://christophergrant.github.io/delta-hydro/delta.html#hydro.delta.bootstrap_scd2)
- Issue queries against Delta Log metadata, quickly and efficently getting things like partition sizes on huge tables - [hydro.delta.partition_stats](https://christophergrant.github.io/delta-hydro/delta.html#hydro.delta.partition_stats)
- Other quality of life improvements like [hydro.delta.detail_enhanced](https://christophergrant.github.io/delta-hydro/delta.html#hydro.delta.detail_enhanced) and [hydro.spark.fields](https://christophergrant.github.io/delta-hydro/spark.html#hydro.spark.fields)

## Contributions ✨

Contributions are welcome. 

However, please [create an issue](https://github.com/christophergrant/delta-hydro/issues/new/choose) before starting work on a feature to make sure that it aligns with the future of the project.

## Naming 🤓

Originally this project was going to be `hydrologist` but that's way too long and pretentious, so we shortened to `hydro`.

A hydrologist is a person who studies water and its movement. Delta Lake, Data Lake, Lakehouse => water.
