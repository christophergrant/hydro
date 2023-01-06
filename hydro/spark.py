from __future__ import annotations

from delta import DeltaTable
from pyspark.sql.types import DataType
from pyspark.sql.types import StructType


def _fields(
    delta_table: DeltaTable,
    include_types: bool,
) -> list[tuple[str, DataType] | str]:
    # ChatGPT 🤖 prompt:
    # write a program that takes a PySpark StructType and returns the leaf node field names, even the nested ones # noqa: E501
    schema = delta_table.toDF().schema

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


def fields(delta_table: DeltaTable) -> list[str]:
    """
    :param delta_table:
    :return:
    """
    return _fields(delta_table, True)


def fields_with_types(delta_table: DeltaTable) -> list[tuple[str, DataType]]:
    """
    :param delta_table:
    :return:
    """
    return _fields(delta_table, False)
