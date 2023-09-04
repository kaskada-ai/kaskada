from __future__ import annotations

from datetime import timedelta
from typing import Union

from . import Arg, Literal, Timestream


def add(self, rhs: Arg) -> Timestream:
    """Return a Timestream adding this and `rhs`.

    Args:
        rhs: The Timestream or literal value to add to this.

    Notes:
        You can also write `a.add(b)` as `a + b`.
    """
    if isinstance(rhs, timedelta):
        # Right now, we can't convert a time delta directly to a scalar value (literal).
        # So we convert it to seconds and then add it.
        # Note that this loses precision if the timedelta has a fractional number of seconds,
        # and fail if the number of seconds exceeds an integer.
        session = self._ffi_expr.session()
        seconds = Timestream._call("seconds", int(rhs.total_seconds()), session=session)
        return Timestream._call("add_time", seconds, self, input=self)
    else:
        return Timestream._call("add", self, rhs, input=self)


def add__(self, rhs: Arg) -> Timestream:
    """Implement `self + rhs`."""
    return self.add(rhs)


def addr__(self, lhs: Arg) -> Timestream:
    """Implement `lhs + self`."""
    if callable(lhs):
        lhs = lhs(self)
    if not isinstance(lhs, Timestream):
        lhs = Timestream._literal(lhs, self._ffi_expr.session())
    return lhs.add(self)


def ceil(self) -> Timestream:
    """Return a Timestream rounding self up to the next largest integer."""
    return Timestream._call("ceil", self, input=self)


def clamp(self, min: Arg = None, max: Arg = None) -> Timestream:
    """Return a Timestream from `self` clamped between `min` and `max`.

    Args:
        min: The literal value to set as the lower bound.
        max: The literal value to set as the upper bound.
    """
    return Timestream._call("clamp", self, min, max, input=self)


def div(self, divisor: Arg) -> Timestream:
    """Return a Timestream by dividing this and `divisor`.

    Args:
        divisor: The Timestream or literal value to divide this by.

    Notes:
        You can also write `a.div(b)` as `a / b`.
    """
    return Timestream._call("div", self, divisor, input=self)


def div__(self, divisor: Arg) -> Timestream:
    """Implement `self / divisor`."""
    return self.div(divisor)


def divr__(self, dividend: Arg) -> Timestream:
    """Implement `dividend / self`."""
    if callable(dividend):
        dividend = dividend(self)
    if not isinstance(dividend, Timestream):
        dividend = Timestream._literal(dividend, self._ffi_expr.session())
    return dividend.div(self)


def exp(self) -> Timestream:
    """Return a Timestream raising `e` to the power of `self`."""
    return Timestream._call("exp", self)


def floor(self) -> Timestream:
    """Return a Timestream of self rounded down to the nearest integer."""
    return Timestream._call("floor", self)


def greatest(self, rhs: Union[Timestream, Literal]) -> Timestream:
    """Return a Timestream with the maximum value of `self` and `rhs` at each point.

    Args:
        rhs: The Timestream or literal value to compare to this.

    Returns:
        Each point contains the value from `self` if `self`
        is greater than `rhs`, otherwise it contains `rhs`.
        If any input is `null` or `NaN`, then that will be
        the result.

    See Also:
        This returns the greatest of two values. See
        :func:`max` for the maximum of values in
        a column.
    """
    return Timestream._call("zip_max", self, rhs)


def least(self, rhs: Arg) -> Timestream:
    """Return a Timestream with the minimum value of `self` and `rhs` at each point.

    Args:
        rhs: The Timestream or literal value to compare to this.

    Returns:
        Each point contains the value from `self` if `self`
        is less than `rhs`, otherwise it contains `rhs`.
        If any input is `null` or `NaN`, then that will be
        the result.

    See Also:
        This returns the least of two values. See
        :func:`min` for the minimum of values in
        a column.
    """
    return Timestream._call("zip_min", self, rhs)


def mul(self, rhs: Arg) -> Timestream:
    """Return a Timestream multiplying this and `rhs`.

    Args:
        rhs: The Timestream or literal value to multiply with this.

    Notes:
        You can also write `a.mul(b)` as `a * b`.
    """
    return Timestream._call("mul", self, rhs, input=self)


def mul__(self, rhs: Arg) -> Timestream:
    """Implement `self * rhs`."""
    return self.mul(rhs)


def mulr__(self, lhs: Arg) -> Timestream:
    """Implement `lhs * self`."""
    if callable(lhs):
        lhs = lhs(self)
    if not isinstance(lhs, Timestream):
        lhs = Timestream._literal(lhs, self._ffi_expr.session())
    return lhs.mul(self)


def neg(self) -> Timestream:
    """Return a Timestream from the numeric negation of self."""
    return Timestream._call("neg", self)


def powf(self, power: Arg) -> Timestream:
    """Return a Timestream raising `self` to the power of `power`.

    Args:
        power: The Timestream or literal value to raise `self` to.
    """
    return Timestream._call("powf", self, power, input=self)


def round(self) -> Timestream:
    """Return a Timestream with all values rounded to the nearest integer.

    Returns:
        A Timestream of the same type as `self`. The result contains `null`
        if the value was `null` at that point. Otherwise, it contains
        the result of rounding the value to the nearest integer.

    Notes:
        This method may be applied to any numeric type. For anything other
        than `float32` and `float64` it has no affect since the values
        are already integers.

    See Also:
        - :func:`ceil`
        - :func:`floor`
    """
    return Timestream._call("round", self)


def sqrt(self) -> Timestream:
    """Return a Timestream with the square root of all values."""
    return Timestream._call("sqrt", self)


def sub(self, rhs: Arg) -> Timestream:
    """Return a Timestream subtracting `rhs` from this.

    Args:
        rhs: The Timestream or literal value to subtract from this.

    Notes:
        You can also write `a.sub(b)` as `a - b`.
    """
    return Timestream._call("sub", self, rhs, input=self)


def sub__(self, rhs: Arg) -> Timestream:
    """Implement `self - rhs`."""
    return self.sub(rhs)


def subr__(self, lhs: Arg) -> Timestream:
    """Implement `lhs - self`."""
    if callable(lhs):
        lhs = lhs(self)
    if not isinstance(lhs, Timestream):
        lhs = Timestream._literal(lhs, self._ffi_expr.session())
    return lhs.sub(self)
