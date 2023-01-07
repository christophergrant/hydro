---
sd_hide_title: true
---

# Overview

```{rubric} hydro - Delta Lake tooling
```

A Python-based toolkit for making data engineering with Delta Lake easier, more productive, and more fun!

```{button-ref} intro
:ref-type: doc
:color: primary
:class: sd-rounded-pill
```

---

::::{grid} 1 2 2 3
:gutter: 1 1 1 2

:::{grid-item-card} {octicon}`git-pull-request-closed;1.5em;sd-mr-1` Deduplication
:link: guides/deduplication
:link-type: doc

Hydro extends the functionality of Delta Lake to support in-place deletions without doing full overwrites.
+++
[Learn more »](guides/deduplication.md)
:::

:::{grid-item-card} {octicon}`fold;1.5em;sd-mr-1` SCDs
:link: guides/scd
:link-type: doc

Use hydro to implement powerful data engineering patterns like Slowly Changing Dimensions.

+++
[Learn more »](guides/scd.md)
:::

:::{grid-item-card} {octicon}`light-bulb;1.5em;sd-mr-1` Table Insights
:link: guides/insights
:link-type: doc

Hydro grants deep insight into the physical structure of Delta Lake tables using blazingly fast techniques.

+++
[Learn more »](guides/insights.md)
:::

::::

---

```{rubric} API reference
```
[hydro.delta](api/delta.md)

[hydro.spark](api/spark.md)


```{rubric} Additional resources
```
[Delta Lake Documentation](https://docs.delta.io/latest/index.html)
: For understanding more about the Delta Lake project

[Delta Lake Python API](https://docs.delta.io/latest/api/python/index.html)
: Delta Lake's Python API documentation


[PySpark API Documentation](https://spark.apache.org/docs/latest/api/python/reference/index.html)
: PySpark is the official Python bindings for Apache Spark

```{rubric} Acknowledgements
```

Delta Lake is supported by the open community, [Delta Lake](https://delta.io/community/).


```{toctree}
:hidden:
intro.md
```

```{toctree}
:hidden:
:caption: Guides

guides/deduplication.md
guides/scd.md
guides/insights.md
```

```{toctree}
:hidden:
:caption: API Reference

api/delta.md
api/spark.md
```

[pypi-link]: https://pypi.org/project/delta-hydro/
