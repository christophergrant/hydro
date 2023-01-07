from __future__ import annotations

from uuid import uuid4

import pyspark.sql.functions as F
from pyspark.sql import Column
from pyspark.sql import DataFrame
from pyspark.sql import Window
from pyspark.sql.types import DataType
from pyspark.sql.types import StructType


def _fields(
    df: DataFrame,
    include_types: bool,
) -> list[tuple[str, DataType] | str]:
    # ChatGPT 🤖 prompt:
    # write a program that takes a PySpark StructType and returns the leaf node field names, even the nested ones # noqa: E501
    schema = df.schema

    def get_leaf_fields(
        struct: StructType,
        include_types: bool,
    ) -> list[tuple[str, DataType] | str]:
        def _get_leaf_fields(
            struct: StructType,
            prefix: str,
        ) -> list[tuple[str, DataType] | str]:
            fields: list[tuple[str, DataType] | str] = []
            for field in struct:
                if isinstance(field.dataType, StructType):
                    fields.extend(
                        _get_leaf_fields(
                            field.dataType,
                            prefix + field.name + '.',
                        ),
                    )
                else:
                    if include_types:
                        fields.append((prefix + field.name, field.dataType))
                    else:
                        fields.append(prefix + field.name)
            return fields

        return _get_leaf_fields(struct, '')

    return get_leaf_fields(schema, include_types)


def fields(df: DataFrame) -> list[str]:
    """

    Returns names of all of the fields of a DataFrame, including nested ones.

    This contrasts with `StructType.fieldNames` as it gives fully qualified names for nested fields.

    :param df: DataFrame that you want to extract all fields from
    :return: A list of column names, all strings
    """
    return _fields(df, False)


def fields_with_types(df: DataFrame) -> list[tuple[str, DataType]]:
    """

    See docs for `fields`.

    Like `fields`, but returns DataType along with field names as a tuple.

    :param df: DataFrame that you want to extract all fields and types from
    :return: A list of tuples of (column_name, type)
    """
    return _fields(df, True)


def deduplicate_dataframe(
    df: DataFrame,
    keys: list[str] | str = None,
    tiebreaking_columns: list[str] = None,
) -> DataFrame:
    """
    Removes duplicates from a Spark DataFrame.

    can we validate without keys? not meaningfully
    is there an instance where we would want to provide keys but also specify full_row dupes?

    :param df: The target Delta Lake table that contains duplicates.
    :param keys: A list of column names used to distinguish rows. The order of this list does not matter.
    :param tiebreaking_columns: A list of column names used for ordering. The order of this list matters, with earlier elements "weighing" more than lesser ones. The columns will be evaluated in descending order. In the event of a tie, you will get non-deterministic results.
    :return: The deduplicated DataFrame
    """
    if keys is None:
        keys = []

    if tiebreaking_columns is None:
        tiebreaking_columns = []

    if isinstance(keys, str):  # pragma: no cover
        keys = [keys]

    if not keys:
        return df.drop_duplicates()

    if df.isStreaming and tiebreaking_columns:
        print('df is streaming, ignoring `tiebreaking_columns`')  # pragma: no cover

    count_col = uuid4().hex  # generate a random column name that is virtually certain to not be in the dataset
    window = Window.partitionBy(keys)

    dupes = df.withColumn(count_col, F.count('*').over(window)).filter(F.col(count_col) > 1).drop(count_col)
    deduped = None
    if tiebreaking_columns and not df.isStreaming:
        row_number_col = uuid4().hex
        tiebreaking_desc = [F.col(col).desc() for col in tiebreaking_columns]  # potential enhancement here
        tiebreaking_window = window.orderBy(tiebreaking_desc)
        deduped = (
            dupes.withColumn(row_number_col, F.row_number().over(tiebreaking_window))  # row_number is non-deterministic in the event of ties
            .filter(F.col(row_number_col) == 1)  # take the top row
            .drop(row_number_col)
        )
    else:
        deduped = dupes.drop_duplicates(keys)
    return deduped


def hash_fields(df: DataFrame, denylist_fields: list[str] = None, algorithm: str = 'md5', num_bits=256) -> Column:
    """

    :param df:
    :param denylist_fields:
    :param algorithm:
    :param num_bits:
    :return:
    """
    supported_algorithms = ['sha1', 'sha2', 'md5']

    if algorithm not in supported_algorithms:
        raise ValueError(f'Algorithm {algorithm} not in supported algorithms {supported_algorithms}')

    all_fields = fields(df)

    if denylist_fields:
        all_fields = list(set(all_fields) - set(denylist_fields))

    if algorithm == 'sha1':
        hash_col = F.sha1(F.concat_ws('', *all_fields))
    elif algorithm == 'sha2':
        hash_col = F.sha2(F.concat_ws('', *all_fields), num_bits)
    else:
        hash_col = F.md5(F.concat_ws('', *all_fields))

    return hash_col
