from __future__ import annotations

import collections
import hashlib
import re
from typing import Callable
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


def hash_fields(df: DataFrame, denylist_fields: list[str] = None, algorithm: str = 'xxhash64', num_bits=256) -> Column:
    """

    Generates a hash digest of all fields.

    :param df: Input dataframe that is to be hashed.
    :param denylist_fields: Fields that will not be hashed.
    :param algorithm: The function that is used to generate the hash digest. Includes sha1, sha2, md5, hash, xxhash64.
    :param num_bits: For SHA2 only. The number of output bits.
    :return: A column that represents the hash.
    """
    supported_algorithms = ['sha1', 'sha2', 'md5', 'hash', 'xxhash64']

    if algorithm not in supported_algorithms:
        raise ValueError(f'Algorithm {algorithm} not in supported algorithms {supported_algorithms}')

    all_fields = fields(df)

    if denylist_fields:
        all_fields = list(set(all_fields) - set(denylist_fields))

    all_fields.sort()
    if algorithm == 'sha1':
        hash_col = F.sha1(F.concat_ws('', *all_fields))
    elif algorithm == 'sha2':
        hash_col = F.sha2(F.concat_ws('', *all_fields), num_bits)
    elif algorithm == 'hash':
        hash_col = F.hash(F.concat_ws('', *all_fields))
    elif algorithm == 'xxhash64':
        hash_col = F.xxhash64(F.concat_ws('', *all_fields))
    else:
        hash_col = F.md5(F.concat_ws('', *all_fields))

    return hash_col


def hash_schema(df: DataFrame, denylist_fields: list[str] = None) -> Column:
    """

    Generates a hash digest of a DataFrame's schema. Uses the hashlib.md5 algorithm.

    :param df: Input dataframe whose schema is to be hashed.
    :param denylist_fields: Fields that will not be hashed.
    :return: A column that represents the hash.
    """

    all_fields = fields(df)
    if denylist_fields:
        all_fields = list(set(all_fields) - set(denylist_fields))

    fields_set = set(all_fields)
    if len(all_fields) != len(fields_set):
        dupes = [item for item, count in collections.Counter(all_fields).items() if count > 1]
        raise ValueError(f'Duplicate field(s) detected in df, {dupes}')

    """
    ChatGPT 🤖 prompt:
     python's hash function seems to not be deterministic across sessions. give me a python program that gives the md5 hash of a string (python 3)
    """
    schema_hash = hashlib.md5(''.join(sorted(all_fields)).encode('utf-8')).hexdigest()
    hash_col = F.lit(schema_hash)  # amalgamate list as string bc list is un-hashable
    return hash_col


def _map_fields(df: DataFrame, fields_to_map: list[str], function: Callable) -> DataFrame:

    for field in fields_to_map:
        df = df.withColumn(field, function(field))
    return df


def map_fields_by_regex(df: DataFrame, regex: str, function: Callable):
    """

    :param df:
    :param regex:
    :param function:
    :return:
    """
    # ChatGPT 🤖 prompt:
    # i have a regex pattern string. write a python program that iterates through a list of strings and returns elements that match the regex
    regex = re.compile(regex)
    all_fields = fields(df)
    matches = [field for field in all_fields if regex.search(field)]
    return _map_fields(df, matches, function)


def map_fields_by_type(df: DataFrame, target_type: DataType, function: Callable):
    """

    :param df:
    :param target_type:
    :param function:
    :return:
    """
    all_fields = fields_with_types(df)
    pertinent_fields = [field[0] for field in all_fields if field[1] == target_type]
    return _map_fields(df, pertinent_fields, function)


def map_fields(df: DataFrame, field_list: list[str], function: F):
    """

    :param df:
    :param field_list:
    :param function:
    :return:
    """
    return _map_fields(df, field_list, function)
