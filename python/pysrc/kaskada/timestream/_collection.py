from __future__ import annotations

import pyarrow as pa

from . import Timestream, Arg

def __getitem__(self, key: Arg) -> Timestream:
    """Implement `self[key]` using `index`.

    See Also:
        index
    """
    return self.index(key)

def flatten(self) -> Timestream:
    """Flatten a list of lists to a list of values."""
    return Timestream._call("flatten", self)

def index(self, key: Arg) -> Timestream:
    """Return a Timestream indexing into the elements of `self`.

    If the Timestream contains lists, the key should be an integer index.

    If the Timestream contains maps, the key should be the same type as the map keys.

    Args:
        key: The key to index into the expression.

    Raises:
        TypeError: When the Timestream is not a record, list, or map.

    Returns:
        Timestream with the resulting value (or `null` if absent) at each point.

    Note:
        Indexing may be written using the operator `self[key]` instead of `self.index(key)`.
    """
    data_type = self.data_type
    if isinstance(data_type, pa.MapType):
        return Timestream._call("get", key, self, input=self)
    elif isinstance(data_type, pa.ListType):
        return Timestream._call("index", key, self, input=self)
    else:
        raise TypeError(f"Cannot index into {data_type}")

def length(self) -> Timestream:
    """Return a Timestream containing the length of `self`.

    Raises:
        TypeError: When the Timestream is not a string or list.
    """
    if self.data_type.equals(pa.string()):
        return Timestream._call("len", self)
    elif isinstance(self.data_type, pa.ListType):
        return Timestream._call("list_len", self)
    else:
        raise TypeError(f"length not supported for {self.data_type}")

def union(self, other: Arg) -> Timestream:
    """Union the lists in this timestream with the lists in the other Timestream.

    This corresponds to a pair-wise union within each row of the timestreams.

    Args:
        other: The Timestream of lists to union with.
    """
    return Timestream._call("union", self, other, input=self)