# hydro 💧

[![main](https://github.com/christophergrant/delta-hydro/actions/workflows/main.yml/badge.svg)](https://github.com/christophergrant/delta-hydro/actions/workflows/main.yml)
[![codecov](https://codecov.io/gh/christophergrant/delta-hydro/branch/main/graph/badge.svg?token=Z64814CV1E)](https://codecov.io/gh/christophergrant/delta-hydro)

hydro is a collection of Python-based [Apache Spark](https://spark.apache.org/) and [Delta Lake](https://delta.io/) tooling.

See [Key Functionality](#key-functionality-) for concrete use cases.

## Warning ⚠️

hydro is not yet ready for production. Use it at your own risk.

## Installation

```commandline
pip install delta-hydro
```

## Docs 📖

https://christophergrant.github.io/delta-hydro

## Key Functionality 🔑

- De-duplicate a Delta Lake table, in-place, without a full overwrite - [hydro.delta.deduplicate](https://christophergrant.github.io/delta-hydro/api/delta.html#hydro.delta.deduplicate)
- Correctly perform [Slowly Changing Dimensions (SCD)](https://en.wikipedia.org/wiki/Slowly_changing_dimension) (types 1 or 2) on Delta Lake tables - [hydro.delta.scd](https://christophergrant.github.io/delta-hydro/api/delta.html#hydro.delta.scd) and [hydro.delta.bootstrap_scd2](https://christophergrant.github.io/delta-hydro/delta.html#hydro.delta.bootstrap_scd2)
- Issue queries against Delta Log metadata, quickly and efficently getting things like partition sizes on huge tables - [hydro.delta.partition_stats](https://christophergrant.github.io/delta-hydro/api/delta.html#hydro.delta.partition_stats)
- Other quality of life improvements like [hydro.delta.detail_enhanced](https://christophergrant.github.io/delta-hydro/api/delta.html#hydro.delta.detail_enhanced) and [hydro.spark.fields](https://christophergrant.github.io/delta-hydro/api/spark.html#hydro.spark.fields)

## Contributions ✨

Contributions are welcome.

However, please [create an issue](https://github.com/christophergrant/delta-hydro/issues/new/choose) before starting work on a feature to make sure that it aligns with the future of the project.

## Naming 🤓

Originally this project was going to be `hydrologist` but that's way too long and pretentious, so we shortened to `hydro`.

A hydrologist is a person who studies water and its movement. Delta Lake, Data Lake, Lakehouse => water.

## ChatGPT and LLMs 🤖

Some of this project's code and documentation was generated by a Large [Language Model](https://en.wikipedia.org/wiki/Language_model)(LLM), namely [ChatGPT](https://chat.openai.com/chat).

We are proud prompt engineers, so we display the prompt that gave us the code in hydro's source ([example](https://github.com/christophergrant/delta-hydro/commit/8d2d84da4930f14caac62c46ea9a1c07a8bdeac4#diff-4665a0f13cae8eb34e13e308ee3935edf0a63f563ac6301038b0d15f95666446R11)).

Our take is that the model is very impressive, but not sophisticated enough to be able to write this whole program (yet). A lot of this stuff is very context-dependent and would be difficult to explain to an AI. Plus, ChatGPT isn't aware of newer APIs as it was trained on an older corpus.

We are excited for the future of humanity given recent advancements in artificial intelligence and hope that the technology is used to [liberate](https://www.4dayweek.com/), rather than accelerate.
