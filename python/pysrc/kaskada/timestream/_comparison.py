from __future__ import annotations

from . import Arg, Timestream


def eq(self, other: Arg) -> Timestream:
    """Return a Timestream that is true if this is equal to `other`.

    Args:
        other: The Timestream or literal value to compare to.

    Note:
        Equality is *not* available as `a == b`.
    """
    return Timestream._call("eq", self, other, input=self)


def ge(self, rhs: Arg) -> Timestream:
    """Return a TimeStream that is true if this is greater than or equal to `rhs`.

    Args:
        rhs: The Timestream or literal value to compare to.

    Notes:
        You can also write `a.ge(b)` as `a >= b`.
    """
    return Timestream._call("gte", self, rhs, input=self)


def ge__(self, rhs: Arg) -> Timestream:
    """Implement `self >= rhs`."""
    return self.ge(rhs)


def gt(self, rhs: Arg) -> Timestream:
    """Return a Timestream that is true if this is greater than `rhs`.

    Args:
        rhs: The Timestream or literal value to compare to.

    Notes:
        You can also write `a.gt(b)` as `a > b`.
    """
    return Timestream._call("gt", self, rhs, input=self)


def gt__(self, rhs: Arg) -> Timestream:
    """Implement `self > rhs`."""
    return self.gt(rhs)


def le(self, rhs: Arg) -> Timestream:
    """Return a Timestream that is true if this is less than or equal to `rhs`.

    Args:
        rhs: The Timestream or literal value to compare to.

    Notes:
        You can also write `a.le(b)` as `a <= b`.
    """
    return Timestream._call("lte", self, rhs, input=self)


def le__(self, rhs: Arg) -> Timestream:
    """Implement `self <= rhs`."""
    return self.le(rhs)


def lt(self, rhs: Arg) -> Timestream:
    """Return a Timestream that is true if this is less than `rhs`.

    Args:
        rhs: The Timestream or literal value to compare to.

    Notes:
        You can also write `a.lt(b)` as `a < b`.
    """
    return Timestream._call("lt", self, rhs, input=self)


def lt__(self, rhs: Arg) -> Timestream:
    """Implement `self < rhs`."""
    return self.lt(rhs)


def ne(self, other: Arg) -> Timestream:
    """Return a Timestream that is true if this is not equal to `other`.

    Args:
        other: The Timestream or literal value to compare to.

    Note:
        Inequality is *not* available as `a != b`.
    """
    return Timestream._call("neq", self, other, input=self)


def is_not_null(self) -> Timestream:
    """Return a boolean Timestream containing `true` when `self` is not `null`."""
    return Timestream._call("is_valid", self)


def is_null(self) -> Timestream:
    """Return a boolean Timestream containing `true` when `self` is `null`."""
    return self.is_not_null().not_()
