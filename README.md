# hydro

hydro is a collection of Apache Spark and Linux Foundation Delta Lake related tooling.

## Install

```commandline
pip install delta-hydro
```

## Usage

### DeltaTable full schema

Sometimes you need to get a delta table's fields. All of them.

Spark lets you do this, but only gives you the top-level fields. Real life data is often more complicated and nested.

`hydro.fields` gives us what we need.

```python

data = """
{
    "isbn":"0-942299-79-5",
    "title":"The Society of the Spectacle",
    "author":{
      "first_name":"Guy",
      "last_name":"Debord"
    },
    "published_year":1967,
    "pages":154,
    "language":"French"
}
"""
# write `data` to delta table and define it as `delta_table`
better_fieldnames = hydro.fields(delta_table)
print(better_fieldnames)
```
results in
```python
['author.first_name', 'author.last_name', 'isbn', 'language', 'pages', 'published_year', 'title']
```

which is much better than what Spark gives us.

### DeltaTable detail
You can get high-level information about a Delta Lake table by using the `describe` command. The issue with this is that the command was built for machines, not humans.

Hydro provides a simple wrapper that makes the output more readable for humans:

```python
delta_table = DeltaTable(spark, path)
hydro.detail(delta_table)
```
results in something like
```
{
    'createdAt': datetime.datetime(2023, 1, 4, 16, 0, 35, 328000),
    'description': None,
    'format': 'delta',
    'id': '8f5fa9f2-fd74-49de-afb6-d6e19d219838',
    'lastModified': datetime.datetime(2023, 1, 4, 16, 0, 52, 755000),
    'location': 'proto:/path/to/delta/table',
    'minReaderVersion': 1,
    'minWriterVersion': 2,
    'name': None,
    'numFiles': '1,000',
    'partitionColumns': [],
    'properties': {},
    'size': '523.39 KiB'
}
```

## Contributions

Contributions are welcome.

## Naming

Originally this project was going to be `hydrologist` but that's way too long and pretentious so we shortened to `hydro`.

A hydrologist is a person who studies water and its movement. Delta Lake, Data Lake, Lakehouse => water.

## Philosophy

Minimalism in almost everything.

Break the rules if the rules are not justified.

Openness above all.
