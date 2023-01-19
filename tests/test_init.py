from __future__ import annotations

import pytest

from hydro import _humanize_timestamp
from tests import _df_to_list_of_dict
from tests import spark


def test_df_to_dict_exception():
    df = spark.range(11)
    with pytest.raises(OverflowError) as exception:
        _df_to_list_of_dict(df)
    assert (
        exception.value.args[0] == 'DataFrame over 10 rows, not materializing. Was this an accident?'
    )


def test_humanize_timestamp_ms():
    result = _humanize_timestamp(1674080199000)
    assert result == '2023-01-18T22:16:39+00:00'


def test_humanize_timestamp_s():
    result = _humanize_timestamp(1674080199)
    assert result == '2023-01-18T22:16:39+00:00'
