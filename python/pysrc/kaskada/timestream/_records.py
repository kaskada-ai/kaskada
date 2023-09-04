from __future__ import annotations

from typing import (
    Callable,
    Mapping,
)

import pyarrow as pa

from . import Timestream, Arg, record

def col(self, name: str) -> Timestream:
    """Return a Timestream accessing the named column or field of `self`.

    Args:
        name: The name of the column or field to access.

    Raises:
        TypeError: When the Timestream is not a record.
    """
    data_type = self.data_type
    if isinstance(data_type, pa.StructType) or isinstance(data_type, pa.ListType):
        return Timestream._call("fieldref", self, name)
    else:
        raise TypeError(
            f"Cannot access column {name!r} of non-record type '{data_type}'"  # noqa : B907
        )

def extend(
    self,
    fields: Timestream
    | Mapping[str, Arg]
    | Callable[[Timestream], Timestream | Mapping[str, Arg]],
) -> Timestream:
    """Return a Timestream containing fields from `self` and `fields`.

    If a field exists in the base Timestream and the `fields`, the value
    from the `fields` will be taken.

    Args:
        fields: Fields to add to each record in the Timestream.
    """
    # This argument order is weird, and we shouldn't need to make a record
    # in order to do the extension.
    if callable(fields):
        fields = fields(self)
    if not isinstance(fields, Timestream):
        fields = record(fields)
    return Timestream._call("extend_record", fields, self, input=self)


def _record(self, fields: Callable[[Timestream], Mapping[str, Arg]]) -> Timestream:
    """Return a record Timestream from fields computed from this timestream.

    Args:
        fields: The fields to include in the record.

    See Also:
        kaskada.record: Function for creating a record from one or more
            timestreams.
    """
    return record(fields(self))

def remove(self, *args: str) -> Timestream:
    """Return a Timestream removing the given fields from `self`.

    Args:
        *args: The field names to remove.
    """
    return Timestream._call("remove_fields", self, *args)

def select(self, *args: str) -> Timestream:
    """Return a Timestream selecting the given fields from `self`.

    Args:
        *args: The field names to select.
    """
    return Timestream._call("select_fields", self, *args)