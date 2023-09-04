from __future__ import annotations

from . import Timestream


def lower(self) -> Timestream:
    """Return a Timestream with all values converted to lower case."""
    return Timestream._call("lower", self)


def upper(self) -> Timestream:
    """Return a Timestream with all values converted to upper case."""
    return Timestream._call("upper", self)
