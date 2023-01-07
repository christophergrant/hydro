```{eval-rst}
.. deduplicate.md:
```

Let's say you have a Delta Lake table with the following contents and schema:

| id  | type  | timestamp           |
|-----|-------|---------------------|
| 1   | watch | 2023-01-01 00:00:00 |
| 2   | click | 2023-01-01 00:00:01 |
| 2   | click | 2023-01-01 00:00:01 |

There are duplicates in the table. This is a data quality issue!

Our first step should be to ensure that we don't get more duplicates. But now that they're already in our table, how do we get them out?

This is where `hydro.delta.deduplicate` comes in:

```python
import hydro.delta as hd
from delta import DeltaTable
delta_table = DeltaTable.forPath(spark, "/some/path")
hd.deduplicate(delta_table, temp_path="/some/temporary/path", keys="id")
```

and after a little bit, we get this:

| id  | type  | timestamp           |
|-----|-------|---------------------|
| 1   | watch | 2023-01-01 00:00:00 |
| 2   | click | 2023-01-01 00:00:01 |

Great! But what about something more advanced?

For example:

| user_id | region_id | data      | timestamp           |
|---------|-----------|-----------|---------------------|
| 1       | 1         | data      | 2023-01-01 01:00:00 |
| 1       | 1         | newdata   | 2023-01-01 01:00:01 |

The data is not exactly duplicated, but it's not what we want. Plus, we have more than one key.

We can do this pretty easily:

```python
import hydro.delta as hd
from delta import DeltaTable
delta_table = DeltaTable.forPath(spark, "/some/path")
hd.deduplicate(delta_table, temp_path="/some/temporary/path", keys=["id", "region_id"], tiebreaking_columns=["timestamp"])
```

And our resulting table looks like:

| user_id | region_id | data      | timestamp           |
|---------|-----------|-----------|---------------------|
| 1       | 1         | newdata   | 2023-01-01 01:00:01 |

**WARNING**

Please note that `deduplicate` occurs over multiple transactions.

If the `deduplicate` operation fails mid-way, you may need to intervene. In the event of failure, we recommend running `RESTORE` on the Delta table to its previous version and try again.

If Delta Lake ever provides a way to disambiguate between rows without relying on the data (e.g an internal rowid), this could be a single transaction and thus, safer.
