from __future__ import annotations

__version__ = '0.2.1'

# transformed numFiles to string, sizeInBytes -> size with type string
from datetime import datetime
from time import time
import pytz

DETAIL_SCHEMA_JSON = '{"fields":[{"metadata":{},"name":"createdAt","nullable":true,"type":"timestamp"},{"metadata":{},"name":"description","nullable":true,"type":"string"},{"metadata":{},"name":"format","nullable":true,"type":"string"},{"metadata":{},"name":"id","nullable":true,"type":"string"},{"metadata":{},"name":"lastModified","nullable":true,"type":"timestamp"},{"metadata":{},"name":"location","nullable":true,"type":"string"},{"metadata":{},"name":"minReaderVersion","nullable":true,"type":"long"},{"metadata":{},"name":"minWriterVersion","nullable":true,"type":"long"},{"metadata":{},"name":"name","nullable":true,"type":"string"},{"metadata":{},"name":"numFiles","nullable":true,"type":"string"},{"metadata":{},"name":"partitionColumns","nullable":true,"type":{"containsNull":true,"elementType":"string","type":"array"}},{"metadata":{},"name":"properties","nullable":true,"type":{"keyType":"string","type":"map","valueContainsNull":true,"valueType":"string"}},{"metadata":{},"name":"size","nullable":true,"type":"string"}],"type":"struct"}'  # noqa: E501


def _humanize_number(number: int) -> str:
    return f'{number:,}'


def _humanize_bytes(num_bytes: float) -> str:
    # ChatGPT 🤖 prompt: "write a python program that converts bytes to their proper units (kb, mb, gb, tb, pb, etc)" # noqa: E501
    units = ['bytes', 'KiB', 'MiB', 'GiB', 'TiB', 'PiB', 'EiB']
    i = 0
    while num_bytes >= 1024 and i < len(units) - 1:
        num_bytes /= 1024
        i += 1
    return f'{num_bytes:.2f} {units[i]}'


def _humanize_timestamp(ts: int, timezone: str = 'UTC') -> str:
    if ts > time() * 10:  # if the timestamp is greater than our current one by a factor of 10, the ts is in ms
        ts = ts / 1000
    tz = pytz.timezone(timezone)
    return datetime.fromtimestamp(ts, tz).isoformat()
