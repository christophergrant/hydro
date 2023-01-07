```{eval-rst}
.. deduplicate.md:
```
# Deduplication

Duplicate data is a common issue in data pipelines that can arise from a variety of sources, including human error, system glitches, or data migration from legacy systems. 
Regardless of the cause, duplicate data can have significant negative impacts on data quality and harm the accuracy, consistency, and reliability of a data asset.

This article focuses on upstream data sources sending duplicates.

---

This article has a couple of sections: 
- Preventing duplicates: strategies for preventing duplicates in your output
- How to deduplicate: instructions for what to do when you already have duplicates in your Delta Lake table

_This article focuses on deduplication with PySpark and Delta Lake, but the concept is similar across engines and table formats._

---
Background information
:::{dropdown} Types of duplicates
Let's say we work at a video content streaming company, where we are receiving user events from our upstream infrastructure.

For some reason, our upstream infrastructure sometimes sends us duplicates. 

It is a good idea to inform the team responsible for this issue that they are sending duplicates downstream to have them fix it. It's also a good idea, generally, to proactively assume that upstream sources may send us duplicates, and to plan accordingly for this worst-case scenario.


### Identical duplicates

| id  | type  | timestamp           | 
|-----|-------|---------------------|
| 1   | watch | 2023-01-15 00:00:00 |
| 1   | watch | 2023-01-15 00:00:00 |

There are two important parts here:
- we are assuming that the `id` column is our primary key that identifies a unique entity in the dataset.
- the entire row is duplicated: all three columns are identical across two rows.

The rows are equivalent in the information they are trying to convey, but they do not accurately represent what happened. It should not be possible to have two separate events for one event `id`.


### Non-identical duplicates

| id  | type  | timestamp               | 
|-----|-------|-------------------------|
| 1   | watch | 2023-01-15 00:00:00     |
| 1   | watch | 2023-01-15 00:00:**01** |

This is similar to the previous case, with one clear distinction: our `id` and `watch` columns match, but `timestamp`  is off by a second.

This distinction is subtle but important.

The rows are roughly equivalent in the information they are trying to convey, but they do not accurately represent what happened. It should not be possible to have two separate events for one event `id`.

:::

---


## Preventing duplicates with Apache Spark and Delta Lake

TODO

---

## Recovering from duplicates with [`hydro.delta.deduplicate`](hydro.delta.deduplicate)
:::{admonition} Data Loss Warning
:class: warning

`deduplicate` is a destructive operation that is not immune to failure. Be careful.
:::

### Warning: recovering from failure

`deduplicate` occurs over two transactions. 

It should be rare, but if the first transaction succeeds and the second does not, your duplicates will be missing, but you will be missing the de-duplicated data.

If this happens, we recommend using [`DeltaTable.restoreToVersion()`](https://docs.delta.io/latest/api/python/index.html#delta.tables.DeltaTable.restoreToVersion) to roll back to the starting version. When `deduplication` starts, hydro will tell you the starting version of the table in `stdout`.

Now that we're clear on the risks and recovery strategy, let's take a look at using `hydro` to solve our duplication issue.

### Identical duplicates
Let's take our example from the above docs and put it into a Delta Lake table.

```python
from pyspark.sql import SparkSession
spark = SparkSession.builder().master("local[*]").getOrCreate()
data = [{"id": 1, "type": "watch", "timestamp": "2023-01-15 00:00:00"},
        {"id": 1, "type": "watch", "timestamp": "2023-01-15 00:00:00"}]
df = spark.createDataFrame(data)
df.write.format("delta").save("/tmp/hydro-dedupe-example")
df.show()
```

results in:

```
+---+-------------------+-----+
| id|          timestamp| type|
+---+-------------------+-----+
|  1|2023-01-15 00:00:00|watch|
|  1|2023-01-15 00:00:00|watch|
+---+-------------------+-----+
```

Now that we have duplicates in our data, what can we do to de-duplicate?

There are several options, but not many good ones. This is where `hydro` can help.

```python
from delta import DeltaTable
import hydro.delta as hd

delta_table = DeltaTable.forPath(spark, "/tmp/hydro-dedupe-example")
hd.deduplicate(delta_table, backup_path="/tmp/hydro-dedupe-backup", keys=["id"])
delta_table.toDF().show()
```
results in:

```
+---+-------------------+-----+
| id|          timestamp| type|
+---+-------------------+-----+
|  1|2023-01-15 00:00:00|watch|
+---+-------------------+-----+
```

This example showed how to deal with identical duplicates, but hydro has the ability to deal with non-identical rows as well.

### Non-identical duplicates


```
+---+-------------------+-----+
| id|          timestamp| type|
+---+-------------------+-----+
|  1|2023-01-15 00:00:00|watch|
|  1|2023-01-15 00:00:01|watch|
+---+-------------------+-----+
```
with code:
```python
from delta import DeltaTable
import hydro.delta as hd

delta_table = DeltaTable.forPath(spark, "/tmp/hydro-dedupe-example2")
hd.deduplicate(delta_table, backup_path="/tmp/hydro-dedupe-backup2", keys=["id"], tiebreaking_columns=["timestamp"])
delta_table.toDF().show()
```

results in:

```
+---+-------------------+-----+
| id|          timestamp| type|
+---+-------------------+-----+
|  1|2023-01-15 00:00:01|watch|
+---+-------------------+-----+
```