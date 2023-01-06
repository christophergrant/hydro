
# hydro.delta

```{eval-rst}
.. autofunction:: hydro.delta.deduplicate
```

Let's say you have a Delta Lake table with the following contents and schema:
    
| id  | type  | timestamp           |
|-----|-------|---------------------|
| 1   | watch | 2023-01-01 00:00:00 |
| 2   | click | 2023-01-01 00:00:01 |
| 2   | click | 2023-01-01 00:00:01 |

There are duplicates in the table! This is a data quality issue.

But now that they're already in our table, how do we get them out? 

This is where `hydro.delta.deduplicate` comes in:

```python
import hydro.delta as hd
from delta import DeltaTable
delta_table = DeltaTable.forPath(spark, "/some/path")
hd.deduplicate(df, "/some/temporary/path", "id")
```

and after a little bit, we get this:

| id  | type  | timestamp           |
|-----|-------|---------------------|
| 1   | watch | 2023-01-01 00:00:00 |
| 2   | click | 2023-01-01 00:00:01 |

Great! But what about something more advanced?

For example: 

| user_id | region_id | timestamp           |
|---------|-----------|---------------------|
| 1       | 1         | 2023-01-01 00:00:00 |
| 1       | 1         | 2023-01-01 00:00:01 |


```{eval-rst}
.. autofunction:: hydro.delta.scd
```



```{eval-rst}
.. automodule:: hydro.delta
    :members:
    :exclude-members: scd, deduplicate
```
