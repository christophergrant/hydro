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

## Types of duplicates

Let's say we work at a video content streaming company, where we are receiving user events from our internal infrastructure:

For some reason, our upstream infrastructure sometimes sends us duplicates. 

It is a good idea to inform the team responsible for this issue that they are sending duplicates downstream to have them fix it. It's also a good idea, generally, to proactively assume that upstream sources may send us duplicates, and to plan accordingly for this worst-case scenario.

### Identical rows

| id  | type  | timestamp           | 
|-----|-------|---------------------|
| 1   | watch | 2023-01-15 00:00:00 |
| 1   | watch | 2023-01-15 00:00:00 |

Two important parts here:
- we are assuming that the `id` column is our primary key that identifies a unique entity in the dataset.
- the entire row is duplicated: all three columns are identical across two rows.

The rows are equivalent in the information they are trying to convey, but they do not accurately represent what happened. It should not be possible to have two separate events for one event `id`.


### Non-identical rows

| id  | type  | timestamp               | 
|-----|-------|-------------------------|
| 1   | watch | 2023-01-15 00:00:00     |
| 1   | watch | 2023-01-15 00:00:**01** |

This is similar to the previous case, with one clear distinction: our `id` and `watch` columns match, but `timestamp`  is off by a second.

This distinction is subtle but important.

The rows are roughly equivalent in the information they are trying to convey, but they do not accurately represent what happened. It should not be possible to have two separate events for one event `id`.

## In-memory deduplication (Prevention)


## On-disk deduplication (Recovery)

