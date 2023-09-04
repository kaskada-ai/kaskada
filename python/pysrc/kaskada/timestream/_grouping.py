from __future__ import annotations

from typing import Optional

from . import Arg, Timestream


def lookup(self, key: Arg) -> Timestream:
    """Return a Timestream looking up the value of `self` for each `key`.

    For each non-`null` point in the `key` timestream, returns the value
    from `self` at that time and associated with that `key`. Returns `null`
    if the `key` is `null` or if there is no `value` computed for that key
    at the corresponding time.

    Args:
        key: The foreign key to lookup. This must match the type of the keys in `self`.
    """
    return Timestream._call("lookup", key, self, input=self)


def with_key(self, key: Arg, grouping: Optional[str] = None) -> Timestream:
    """Return a Timestream with a new grouping by `key`.

    Args:
        key: The new key to use for the grouping.
        grouping: A string literal naming the new grouping. If no `grouping` is specified,
            one will be computed from the type of the `key`.
    """
    return Timestream._call("with_key", key, self, grouping, input=self)
