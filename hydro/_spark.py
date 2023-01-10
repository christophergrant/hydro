from __future__ import annotations

import re
from collections import defaultdict
from copy import copy
from typing import Callable

from pyspark.sql import Column
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DataType
from pyspark.sql.types import StructType


class _DeconstructedField:
    def __init__(self, field: str):
        split_field = field.split('.')
        self.levels = copy(split_field)
        self.trunk = split_field.pop(0)
        if len(split_field) > 1:
            *self.branches, self.leaf = split_field
        elif len(split_field) == 0:
            self.branches, self.leaf = [], None
        else:
            self.branches, self.leaf = [], split_field[0]
        self.trunk_and_branches = '.'.join([self.trunk] + self.branches)


def _field_trie(fields: list[str]):
    result = defaultdict(list)
    for field in fields:
        deconstructed_field = _DeconstructedField(field)
        trunk_and_branches = deconstructed_field.trunk_and_branches
        leaf = deconstructed_field.leaf
        result[trunk_and_branches].append(leaf)
    return result


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


def _get_fields_by_regex(df: DataFrame, regex: str) -> list[str]:
    # ChatGPT 🤖 prompt:
    # i have a regex pattern string. write a python program that iterates through a list of strings and returns elements that match the regex
    regex = re.compile(regex)
    all_fields = _fields(df, False)
    matches = [field for field in all_fields if regex.search(field)]
    return matches


def _get_fields_by_type(df: DataFrame, target_type: DataType) -> list[str]:
    all_fields = _fields(df, True)
    pertinent_fields = [field[0] for field in all_fields if field[1] == target_type]
    return pertinent_fields


def _map_fields(df: DataFrame, fields_to_map: list[str], function: Callable) -> DataFrame:
    for field in fields_to_map:
        df = df.withColumn(field, function(field))
    return df


def _create_drop_field_column(fields_to_drop: tuple[str, list[str | None]]) -> tuple[str, Column]:

    address, leaves = fields_to_drop

    if not leaves or leaves[0] is None:
        raise ValueError(f'Cannot drop top-level field `{address}` with this function. Use df.drop() instead.')

    def _traverse_nest(nest, l, c=0):
        if len(l) == 0:  # termination condition
            return F.col(nest).dropFields(*leaves)
        else:  # recursive step
            current_level = l[0]
            return F.col(nest).withField(current_level, _traverse_nest(f'{nest}.{current_level}', l[1:], c + 1))

    levels = address.split('.')
    if len(levels) == 1:
        return address, F.col(address).dropFields(*leaves)
    col = _traverse_nest(levels[0], levels[1:])
    return levels[0], col
