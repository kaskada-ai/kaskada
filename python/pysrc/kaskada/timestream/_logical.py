from __future__ import annotations

from . import Arg, Timestream


def and_(self, rhs: Arg) -> Timestream:
    """Return the logical conjunction of this Timestream and `rhs`.

    Args:
        rhs: The Timestream or literal value to conjoin with.
    """
    return Timestream._call("logical_and", self, rhs, input=self)


def not_(self) -> Timestream:
    """Return the logical negation of this Timestream."""
    return Timestream._call("not", self)


def or_(self, rhs: Arg) -> Timestream:
    """Return the logical disjunction of this Timestream and `rhs`.

    Args:
        rhs: The Timestream or literal value to disjoin with.
    """
    return Timestream._call("logical_or", self, rhs, input=self)
