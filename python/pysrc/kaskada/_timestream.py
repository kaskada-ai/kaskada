"""Defines classes representing Kaskada expressions."""

from __future__ import annotations

import sys
import warnings
from datetime import datetime, timedelta
from typing import (
    Callable,
    List,
    Literal,
    Mapping,
    Optional,
    Sequence,
    Tuple,
    Union,
    final,
    overload,
)

import kaskada as kd
import kaskada._ffi as _ffi
import pandas as pd
import pyarrow as pa
from typing_extensions import TypeAlias

from ._execution import Execution, ResultIterator, _ExecutionOptions


#: A literal value that can be used as an argument to a Timestream operation.
LiteralValue: TypeAlias = Optional[Union[int, str, float, bool, timedelta, datetime]]

#: A Timestream or literal which can be used as an argument to a Timestream operation.
Arg: TypeAlias = Union[
    "Timestream", Callable[["Timestream"], "Timestream"], LiteralValue
]


def _augment_error(args: Sequence[Arg], e: Exception) -> Exception:
    """Augment an error with information about the arguments."""
    if sys.version_info >= (3, 11):
        # If we can add notes to the exception, indicate the types.
        # This works in Python >=3.11
        for n, arg in enumerate(args):
            if isinstance(arg, Timestream):
                e.add_note(f"Arg[{n}]: Timestream[{arg.data_type}]")
            else:
                e.add_note(f"Arg[{n}]: Literal {arg} ({type(arg)})")
    return e


def _extract_arg(
    arg: Arg, input: Optional[Timestream], session: _ffi.Session
) -> _ffi.Expr:
    """Extract the FFI expression from an argument."""
    if callable(arg):
        if input is None:
            raise ValueError(
                "Cannot use a callable argument without an input Timestream"
            )
        else:
            arg = arg(input)

    if isinstance(arg, Timestream):
        return arg._ffi_expr
    else:
        return Timestream._literal(arg, session)._ffi_expr


class Timestream(object):
    """A `Timestream` represents a computation producing a Timestream."""

    _ffi_expr: _ffi.Expr

    def __init__(self, ffi: _ffi.Expr) -> None:
        """Create a new expression."""
        self._ffi_expr = ffi

    @staticmethod
    def _literal(value: LiteralValue, session: _ffi.Session) -> Timestream:
        """Construct a Timestream for a literal value."""
        if isinstance(value, datetime):
            raise TypeError("Cannot create a literal Timestream from a datetime")

        if isinstance(value, timedelta):
            # The smallest unit stored in timedelta is us
            us = value / timedelta(microseconds=1)
            # Get the seconds as an integer
            seconds = int(us / 1_000_000)
            # Get the leftover nanoseconds
            nanoseconds = int((us % 1_000_000) * 1_000)
            return Timestream(
                _ffi.Expr.literal_timedelta(session, seconds, nanoseconds)
            )
        else:
            return Timestream(_ffi.Expr.literal(session, value))

    @staticmethod
    def _call(
        func: Union[str, _ffi.Udf],
        *args: Arg,
        input: Optional[Timestream] = None,
        session: Optional[_ffi.Session] = None,
    ) -> Timestream:
        """Construct a new Timestream by calling the given function.

        Args:
            func: Name of the function to apply.
            input: The input to use for any "deferred" arguments.
              If `None`, then any arguments that require a `Timestream` argument
              will produce an error.
            *args: List of arguments to the expression.
            session: FFI Session to create the expression in.
              If unspecified, will infer from the arguments.
              Will fail if all arguments are literals and the session is not provided.

        Returns:
            Timestream representing the result of the function applied to the arguments.

        Raises:
            # noqa: DAR401 _augment_error
            TypeError: If the argument types are invalid for the given function.
            ValueError: If the argument values are invalid for the given function.
        """
        # Determine the session. This is unfortunately necessary for creating literals
        # and other expressions.
        if session is None:
            if input is None:
                # If there is no input, then at least one of the arguments must be a
                # Timestream. Specifically, we shouldn't have all literals, nor should
                # we have all Callables that produce Timestreams.
                try:
                    session = next(
                        arg._ffi_expr.session()
                        for arg in args
                        if isinstance(arg, Timestream)
                    )
                except StopIteration as e:
                    raise ValueError(
                        "Cannot determine session from only literal arguments. "
                        "Please provide a session explicitly or use at least one non-literal."
                    ) from e
            else:
                # If there is a session, it is possible (but unlikely) that all arguments
                # are Callables. To handle this, we need to determine the session from the
                # input.
                session = input._ffi_expr.session()

        ffi_args = [_extract_arg(arg, input=input, session=session) for arg in args]
        try:
            if isinstance(func, str):
                return Timestream(
                    _ffi.Expr.call(session=session, operation=func, args=ffi_args)
                )
            elif isinstance(func, _ffi.Udf):
                return Timestream(
                    _ffi.Expr.call_udf(session=session, udf=func, args=ffi_args)
                )
            else:
                raise TypeError(
                    f"invalid type for func. Expected str or udf, saw: {type(func)}"
                )
        except TypeError as e:
            # noqa: DAR401
            raise _augment_error(args, TypeError(str(e))) from e
        except ValueError as e:
            raise _augment_error(args, ValueError(str(e))) from e

    @property
    def data_type(self) -> pa.DataType:
        """The PyArrow type of values in this Timestream."""
        return self._ffi_expr.data_type()

    @property
    def is_continuous(self) -> bool:
        """Returns true if this Timestream is continuous."""
        return self._ffi_expr.is_continuous()

    @final
    def pipe(
        self,
        func: Union[Callable[..., Timestream], Tuple[Callable[..., Timestream], str]],
        *args: Arg,
        **kwargs: Arg,
    ) -> Timestream:
        """Apply chainable functions that produce Timestreams.

        Args:
            func: Function to apply to this Timestream.
              Alternatively a `(func, keyword)` tuple where `keyword` is a string
              indicating the keyword of `func` that expects the Timestream.
            *args: Positional arguments passed into ``func``.
            **kwargs: A dictionary of keyword arguments passed into ``func``.

        Returns:
            The result of applying `func` to the arguments.

        Raises:
            ValueError: When using `self` with a specific `keyword` if the `keyword` also
              appears on in the `kwargs`.

        Notes:
            Use ``.pipe`` when chaining together functions that expect Timestreams.

        Examples:
            Instead of writing

            >>> func(g(h(df), arg1=a), arg2=b, arg3=c)  # doctest: +SKIP

            You can write

            >>> (df.pipe(h)
            >>>    .pipe(g, arg1=a)
            >>>    .pipe(func, arg2=b, arg3=c)
            >>> )  # doctest: +SKIP

            If you have a function that takes the data as (say) the second
            argument, pass a tuple indicating which keyword expects the
            data. For example, suppose ``func`` takes its data as ``arg2``:

            >>> (df.pipe(h)
            >>>    .pipe(g, arg1=a)
            >>>    .pipe((func, 'arg2'), arg1=a, arg3=c)
            >>>  )  # doctest: +SKIP
        """
        if isinstance(func, tuple):
            func, target = func
            if target in kwargs:
                msg = f"{target} is both the pipe target and a keyword argument"
                raise ValueError(msg)
            kwargs[target] = self
            return func(*args, **kwargs)
        else:
            return func(self, *args, **kwargs)

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
            seconds = Timestream._call(
                "seconds", int(rhs.total_seconds()), session=session
            )
            return Timestream._call("add_time", seconds, self, input=self)
        else:
            return Timestream._call("add", self, rhs, input=self)

    def __add__(self, rhs: Arg) -> Timestream:
        """Implement `self + rhs`."""
        return self.add(rhs)

    def __radd__(self, lhs: Arg) -> Timestream:
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

    def sub(self, rhs: Arg) -> Timestream:
        """Return a Timestream subtracting `rhs` from this.

        Args:
            rhs: The Timestream or literal value to subtract from this.

        Notes:
            You can also write `a.sub(b)` as `a - b`.
        """
        return Timestream._call("sub", self, rhs, input=self)

    def __sub__(self, rhs: Arg) -> Timestream:
        """Implement `self - rhs`."""
        return self.sub(rhs)

    def __rsub__(self, lhs: Arg) -> Timestream:
        """Implement `lhs - self`."""
        if callable(lhs):
            lhs = lhs(self)
        if not isinstance(lhs, Timestream):
            lhs = Timestream._literal(lhs, self._ffi_expr.session())
        return lhs.sub(self)

    def exp(self) -> Timestream:
        """Return a Timestream raising `e` to the power of `self`."""
        return Timestream._call("exp", self)

    def floor(self) -> Timestream:
        """Return a Timestream of self rounded down to the nearest integer."""
        return Timestream._call("floor", self)

    def hash(self) -> Timestream:
        """Return a Timestream containing the hash of the input.

        Notes:
            This will only return `null` when interpolated at points where
            the input is not defined. At other points, it will return the
            hash of `null`, which is `0` (not `null`).
        """
        return Timestream._call("hash", self)

    def lower(self) -> Timestream:
        """Return a Timestream with all values converted to lower case."""
        return Timestream._call("lower", self)

    def mul(self, rhs: Arg) -> Timestream:
        """Return a Timestream multiplying this and `rhs`.

        Args:
            rhs: The Timestream or literal value to multiply with this.

        Notes:
            You can also write `a.mul(b)` as `a * b`.
        """
        return Timestream._call("mul", self, rhs, input=self)

    def __mul__(self, rhs: Arg) -> Timestream:
        """Implement `self * rhs`."""
        return self.mul(rhs)

    def __rmul__(self, lhs: Arg) -> Timestream:
        """Implement `lhs * self`."""
        if callable(lhs):
            lhs = lhs(self)
        if not isinstance(lhs, Timestream):
            lhs = Timestream._literal(lhs, self._ffi_expr.session())
        return lhs.mul(self)

    def powf(self, power: Arg) -> Timestream:
        """Return a Timestream raising `self` to the power of `power`.

        Args:
            power: The Timestream or literal value to raise `self` to.
        """
        return Timestream._call("powf", self, power, input=self)

    def div(self, divisor: Arg) -> Timestream:
        """Return a Timestream by dividing this and `divisor`.

        Args:
            divisor: The Timestream or literal value to divide this by.

        Notes:
            You can also write `a.div(b)` as `a / b`.
        """
        return Timestream._call("div", self, divisor, input=self)

    def __truediv__(self, divisor: Arg) -> Timestream:
        """Implement `self / divisor`."""
        return self.div(divisor)

    def __rtruediv__(self, dividend: Arg) -> Timestream:
        """Implement `dividend / self`."""
        if callable(dividend):
            dividend = dividend(self)
        if not isinstance(dividend, Timestream):
            dividend = Timestream._literal(dividend, self._ffi_expr.session())
        return dividend.div(self)

    def lt(self, rhs: Arg) -> Timestream:
        """Return a Timestream that is true if this is less than `rhs`.

        Args:
            rhs: The Timestream or literal value to compare to.

        Notes:
            You can also write `a.lt(b)` as `a < b`.
        """
        return Timestream._call("lt", self, rhs, input=self)

    def __lt__(self, rhs: Arg) -> Timestream:
        """Implement `self < rhs`."""
        return self.lt(rhs)

    def le(self, rhs: Arg) -> Timestream:
        """Return a Timestream that is true if this is less than or equal to `rhs`.

        Args:
            rhs: The Timestream or literal value to compare to.

        Notes:
            You can also write `a.le(b)` as `a <= b`.
        """
        return Timestream._call("lte", self, rhs, input=self)

    def __le__(self, rhs: Arg) -> Timestream:
        """Implement `self <= rhs`."""
        return self.le(rhs)

    def gt(self, rhs: Arg) -> Timestream:
        """Return a Timestream that is true if this is greater than `rhs`.

        Args:
            rhs: The Timestream or literal value to compare to.

        Notes:
            You can also write `a.gt(b)` as `a > b`.
        """
        return Timestream._call("gt", self, rhs, input=self)

    def __gt__(self, rhs: Arg) -> Timestream:
        """Implement `self > rhs`."""
        return self.gt(rhs)

    def ge(self, rhs: Arg) -> Timestream:
        """Return a TimeStream that is true if this is greater than or equal to `rhs`.

        Args:
            rhs: The Timestream or literal value to compare to.

        Notes:
            You can also write `a.ge(b)` as `a >= b`.
        """
        return Timestream._call("gte", self, rhs, input=self)

    def __ge__(self, rhs: Arg) -> Timestream:
        """Implement `self >= rhs`."""
        return self.ge(rhs)

    def and_(self, rhs: Arg) -> Timestream:
        """Return the logical conjunction of this Timestream and `rhs`.

        Args:
            rhs: The Timestream or literal value to conjoin with.
        """
        return Timestream._call("logical_and", self, rhs, input=self)

    def or_(self, rhs: Arg) -> Timestream:
        """Return the logical disjunction of this Timestream and `rhs`.

        Args:
            rhs: The Timestream or literal value to disjoin with.
        """
        return Timestream._call("logical_or", self, rhs, input=self)

    def not_(self) -> Timestream:
        """Return the logical negation of this Timestream."""
        return Timestream._call("not", self)

    def eq(self, other: Arg) -> Timestream:
        """Return a Timestream that is true if this is equal to `other`.

        Args:
            other: The Timestream or literal value to compare to.

        Note:
            Equality is *not* available as `a == b`.
        """
        return Timestream._call("eq", self, other, input=self)

    def ne(self, other: Arg) -> Timestream:
        """Return a Timestream that is true if this is not equal to `other`.

        Args:
            other: The Timestream or literal value to compare to.

        Note:
            Inequality is *not* available as `a != b`.
        """
        return Timestream._call("neq", self, other, input=self)

    def __eq__(self, other: object) -> bool:
        """Warn when Timestreams are compared using `==`."""
        warnings.warn(
            "Using '==' with Timestreams doesn't produce a boolean stream. Use 'eq' instead.",
            stacklevel=2,
        )
        return super().__eq__(other)

    def __ne__(self, other: object) -> bool:
        """Warn when Timestreams are compared using `!=`."""
        warnings.warn(
            "Using '!=' with Timestreams doesn't produce a boolean stream. Use 'ne' instead.",
            stacklevel=2,
        )
        return super().__ne__(other)

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

    def __getitem__(self, key: Arg) -> Timestream:
        """Implement `self[key]` using `index`.

        See Also:
            index
        """
        return self.index(key)

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

    def select(self, *args: str) -> Timestream:
        """Return a Timestream selecting the given fields from `self`.

        Args:
            *args: The field names to select.
        """
        return Timestream._call("select_fields", self, *args)

    def remove(self, *args: str) -> Timestream:
        """Return a Timestream removing the given fields from `self`.

        Args:
            *args: The field names to remove.
        """
        return Timestream._call("remove_fields", self, *args)

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

    def neg(self) -> Timestream:
        """Return a Timestream from the numeric negation of self."""
        return Timestream._call("neg", self)

    def is_null(self) -> Timestream:
        """Return a boolean Timestream containing `true` when `self` is `null`."""
        return self.is_not_null().not_()

    def is_not_null(self) -> Timestream:
        """Return a boolean Timestream containing `true` when `self` is not `null`."""
        return Timestream._call("is_valid", self)

    def filter(self, condition: Arg) -> Timestream:
        """Return a Timestream containing only the points where `condition` is `true`.

        Args:
            condition: The condition to filter on.
        """
        return Timestream._call("when", condition, self)

    def collect(
        self,
        *,
        max: Optional[int],
        min: Optional[int] = 0,
        window: Optional[kd.windows.Window] = None,
    ) -> Timestream:
        """Return a Timestream collecting up to the last `max` values in the `window`.

        Collects the values for each key separately.

        Args:
            max: The maximum number of values to collect.
              If `None` all values are collected.
            min: The minimum number of values to collect before producing a value.
              Defaults to 0.
            window: The window to use for the aggregation. If not specified,
              the entire Timestream is used.

        Returns:
            A Timestream containing the list of collected elements at each point.
        """
        if pa.types.is_list(self.data_type):
            return (
                record({"value": self})
                .collect(max=max, min=min, window=window)
                .col("value")
            )
        else:
            return _aggregation("collect", self, window, max, min)

    def time(self) -> Timestream:
        """Return a Timestream containing the time of each point."""
        return Timestream._call("time_of", self)

    def lag(self, n: int) -> Timestream:
        """Return a Timestream containing the value `n` points before each point.

        Args:
            n: The number of points to lag by.
        """
        # hack to support structs/lists (as collect supports lists)
        return self.collect(max=n + 1, min=n + 1)[0]

    def if_(self, condition: Arg) -> Timestream:
        """Return a `Timestream` from `self` at points where `condition` is `true`, and `null` otherwise.

        Args:
            condition: The condition to check.
        """
        return Timestream._call("if", condition, self, input=self)

    def null_if(self, condition: Arg) -> Timestream:
        """Return a `Timestream` from `self` at points where `condition` is not `false`, and `null` otherwise.

        Args:
            condition: The condition to check.
        """
        return Timestream._call("null_if", condition, self, input=self)

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

    def with_key(self, key: Arg, grouping: Optional[str] = None) -> Timestream:
        """Return a Timestream with a new grouping by `key`.

        Args:
            key: The new key to use for the grouping.
            grouping: A string literal naming the new grouping. If no `grouping` is specified,
              one will be computed from the type of the `key`.
        """
        return Timestream._call("with_key", key, self, grouping, input=self)

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

    def coalesce(self, arg: Arg, *args: Arg) -> Timestream:
        """Return a Timestream containing the first non-null point from self and the arguments.

        Args:
            arg: The next value to be coalesced (required).
            args: Additional values to be coalesced (optional).

        Returns:
            Timestream containing the first non-null value from each point.
            If all values are null, then returns null.
        """
        return Timestream._call("coalesce", self, arg, *args, input=self)

    def shift_to(self, time: Arg) -> Timestream:
        """Return a Timestream shifting each point forward to `time`.

        If multiple values are shifted to the same time, they will be emitted in
        the order in which they originally occurred.

        Args:
            time: The time to shift to. This must be a datetime or a Timestream of timestamp_ns.

        Raises:
            NotImplementedError: When `time` is a datetime (shift_to literal not yet implemented).
        """
        if isinstance(time, datetime):
            # session = self._ffi_expr.session()
            # time_ns = time.timestamp() * 1e9
            # time_ns = Timestream._literal(time_ns, session=session)
            # time_ns = Timestream.cast(time_ns, pa.timestamp('ns'))
            # return Timestream._call("shift_to", time_ns, self)
            raise NotImplementedError("shift_to with datetime literal unsupported")
        else:
            return Timestream._call("shift_to", time, self, input=self)

    def shift_by(self, delta: Arg) -> Timestream:
        """Return a Timestream shifting each point forward by the `delta`.

        If multiple values are shifted to the same time, they will be emitted in
        the order in which they originally occurred.

        Args:
            delta: The delta to shift the point forward by.
        """
        if isinstance(delta, timedelta):
            session = self._ffi_expr.session()
            seconds = Timestream._call(
                "seconds", int(delta.total_seconds()), session=session
            )
            return Timestream._call("shift_by", seconds, self, input=self)
        else:
            return Timestream._call("shift_by", delta, self, input=self)

    def shift_until(self, predicate: Arg) -> Timestream:
        """Return a Timestream shifting each point forward to the next time `predicate` is true.

        Note that if the `predicate` evaluates to true at the same time as `self`,
        the point will be emitted at that time.

        If multiple values are shifted to the same time, they will be emitted in
        the order in which they originally occurred.

        Args:
            predicate: The predicate to determine whether to emit shifted rows.
        """
        return Timestream._call("shift_until", predicate, self, input=self)

    def sum(self, *, window: Optional[kd.windows.Window] = None) -> Timestream:
        """Return a Timestream summing the values in the `window`.

        Computes the sum for each key separately.

        Args:
            window: The window to use for the aggregation. Defaults to the entire Timestream.
        """
        return _aggregation("sum", self, window)

    def first(self, *, window: Optional[kd.windows.Window] = None) -> Timestream:
        """Return a Timestream containing the first value in the `window`.

        Computed for each key separately.

        Args:
            window: The window to use for the aggregation. Defaults to the entire Timestream.
        """
        return _aggregation("first", self, window)

    def last(self, window: Optional[kd.windows.Window] = None) -> Timestream:
        """Return a Timestream containing the last value in the `window`.

        Computed for each key separately.

        Args:
            window: The window to use for the aggregation. Defaults to the entire Timestream.
        """
        return _aggregation("last", self, window)

    def count(self, window: Optional[kd.windows.Window] = None) -> Timestream:
        """Return a Timestream containing the count value in the `window`.

        Computed for each key separately.

        Args:
            window: The window to use for the aggregation. Defaults to the entire Timestream.
        """
        return _aggregation("count", self, window)

    def count_if(self, window: Optional[kd.windows.Window] = None) -> Timestream:
        """Return a Timestream containing the count of `true` values in `window`.

        Computed for each key separately.

        Args:
            window: The window to use for the aggregation. Defaults to the entire Timestream.
        """
        return _aggregation("count_if", self, window)

    def max(self, window: Optional[kd.windows.Window] = None) -> Timestream:
        """Return a Timestream containing the max value in the `window`.

        Computed for each key separately.

        Args:
            window: The window to use for the aggregation. Defaults to the entire Timestream.

        See Also:
            This returns the maximum of values in a column. See
            :func:`greatest` to get the maximum value
            between Timestreams at each point.
        """
        return _aggregation("max", self, window)

    def min(self, window: Optional[kd.windows.Window] = None) -> Timestream:
        """Return a Timestream containing the min value in the `window`.

        Computed for each key separately.

        Args:
            window: The window to use for the aggregation. Defaults to the entire Timestream.

        See Also:
            This returns the minimum of values in a column. See
            :func:`least` to get the minimum value
            between Timestreams at each point.
        """
        return _aggregation("min", self, window)

    def mean(self, window: Optional[kd.windows.Window] = None) -> Timestream:
        """Return a Timestream containing the mean value in the `window`.

        Computed for each key separately.

        Args:
            window: The window to use for the aggregation. Defaults to the entire Timestream.
        """
        return _aggregation("mean", self, window)

    def stddev(self, window: Optional[kd.windows.Window] = None) -> Timestream:
        """Return a Timestream containing the standard deviation in the `window`.

        Computed for each key separately.

        Args:
            window: The window to use for the aggregation. Defaults to the entire Timestream.
        """
        return _aggregation("stddev", self, window)

    def variance(self, window: Optional[kd.windows.Window] = None) -> Timestream:
        """Return a Timestream containing the variance in the `window`.

        Computed for each key separately.

        Args:
            window: The window to use for the aggregation. Defaults to the entire Timestream.
        """
        return _aggregation("variance", self, window)

    def cast(self, data_type: pa.DataType) -> Timestream:
        """Return this Timestream cast to the given data type.

        Args:
            data_type: The DataType to cast to.
        """
        return Timestream(self._ffi_expr.cast(data_type))

    def else_(self, other: Timestream) -> Timestream:
        """Return a Timestream containing `self` when not `null`, and `other` otherwise.

        Args:
            other: The Timestream to use if self is `null`.
        """
        return Timestream._call("else", other, self)

    def seconds_since(self, time: Arg) -> Timestream:
        """Return a Timestream containing seconds between `time` and `self`.

        If `self.time()` is greater than `time`, the result will be positive.

        Args:
            time: The time to compute the seconds since.

                This can be either a stream of timestamps or a datetime literal.
                If `time` is a Timestream, the result will contain the seconds
                from `self.time()` to `time.time()` for each point.
        """
        if isinstance(time, datetime):
            session = self._ffi_expr.session()
            nanos = Timestream._literal(time.timestamp() * 1e9, session=session)
            nanos = Timestream.cast(nanos, pa.timestamp("ns", None))
            return Timestream._call("seconds_between", nanos, self)
        else:
            return Timestream._call("seconds_between", time, self)

    def seconds_since_previous(self, n: int = 1) -> Timestream:
        """Return a Timestream containing seconds between `self` and the time `n` points ago.

        Args:
            n: The number of points to look back. For example, `n=1` refers to
              the previous point.

              Defaults to 1 (the previous point).
        """
        time_of_current = Timestream._call("time_of", self).cast(pa.int64())
        time_of_previous = Timestream._call("time_of", self).lag(n).cast(pa.int64())

        # `time_of` returns nanoseconds, so divide to get seconds
        return time_of_current.sub(time_of_previous).div(1e9).cast(pa.duration("s"))

    def flatten(self) -> Timestream:
        """Flatten a list of lists to a list of values."""
        return Timestream._call("flatten", self)

    def union(self, other: Arg) -> Timestream:
        """Union the lists in this timestream with the lists in the other Timestream.

        This corresponds to a pair-wise union within each row of the timestreams.

        Args:
            other: The Timestream of lists to union with.
        """
        return Timestream._call("union", self, other, input=self)

    def record(self, fields: Callable[[Timestream], Mapping[str, Arg]]) -> Timestream:
        """Return a record Timestream from fields computed from this timestream.

        Args:
            fields: The fields to include in the record.

        See Also:
            kaskada.record: Function for creating a record from one or more
              timestreams.
        """
        return record(fields(self))

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

    def upper(self) -> Timestream:
        """Return a Timestream with all values converted to upper case."""
        return Timestream._call("upper", self)

    def greatest(self, rhs: Arg) -> Timestream:
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

    def preview(
        self,
        limit: int = 10,
        results: Optional[Union[kd.results.History, kd.results.Snapshot]] = None,
    ) -> pd.DataFrame:
        """Preview the points in this TimeStream as a DataFrame.

        Args:
            limit: The number of points to preview.
            results: The results to produce. Defaults to `Histroy()` producing all points.
        """
        return self.to_pandas(results, row_limit=limit)

    def to_pandas(
        self,
        results: Optional[Union[kd.results.History, kd.results.Snapshot]] = None,
        *,
        row_limit: Optional[int] = None,
    ) -> pd.DataFrame:
        """Execute the TimeStream with the given options and return a DataFrame.

        Args:
            results: The results to produce in the DataFrame. Defaults to `History()` producing all points.
            row_limit: The maximum number of rows to return. Defaults to `None` for no limit.
            max_batch_size: The maximum number of rows to return in each batch.
              Defaults to `None` for no limit.

        See Also:
            - :func:`preview`: For quick peeks at the contents of a TimeStream during development.
            - :func:`write`: For writing results to supported destinations without passing through
              Pandas.
            - :func:`run_iter`: For non-blocking (iterator or async iterator) execution.
        """
        execution = self._execute(results, row_limit=row_limit)
        batches = execution.collect_pyarrow()
        table = pa.Table.from_batches(batches, schema=execution.schema())
        table = table.drop_columns(["_subsort", "_key_hash"])
        return table.to_pandas()

    def write(
        self,
        destination: kd.destinations.Destination,
        mode: Literal["once", "live"] = "once",
        results: Optional[Union[kd.results.History, kd.results.Snapshot]] = None,
    ) -> Execution:
        """Execute the TimeStream writing to the given destination.

        Args:
            destination: The destination to write to.
            mode: The execution mode to use. Defaults to `'once'` to produce the results
              from the currently available data. Use `'live'` to start a standing query
              that continues to process new data until stopped.
            results: The results to produce. Defaults to `Histroy()` producing all points.

        Returns:
            An `ExecutionProgress` which allows iterating (synchronously or asynchronously)
            over the progress information, as well as cancelling the query if it is no longer
            needed.
        """
        raise NotImplementedError

    @overload
    def run_iter(
        self,
        kind: Literal["pandas"] = "pandas",
        *,
        mode: Literal["once", "live"] = "once",
        results: Optional[Union[kd.results.History, kd.results.Snapshot]] = None,
        row_limit: Optional[int] = None,
        max_batch_size: Optional[int] = None,
    ) -> ResultIterator[pd.DataFrame]:
        ...

    @overload
    def run_iter(
        self,
        kind: Literal["pyarrow"],
        *,
        mode: Literal["once", "live"] = "once",
        results: Optional[Union[kd.results.History, kd.results.Snapshot]] = None,
        row_limit: Optional[int] = None,
        max_batch_size: Optional[int] = None,
    ) -> ResultIterator[pa.RecordBatch]:
        ...

    @overload
    def run_iter(
        self,
        kind: Literal["row"],
        *,
        mode: Literal["once", "live"] = "once",
        results: Optional[Union[kd.results.History, kd.results.Snapshot]] = None,
        row_limit: Optional[int] = None,
        max_batch_size: Optional[int] = None,
    ) -> ResultIterator[dict]:
        ...

    def run_iter(
        self,
        kind: Literal["pandas", "pyarrow", "row"] = "pandas",
        *,
        mode: Literal["once", "live"] = "once",
        results: Optional[Union[kd.results.History, kd.results.Snapshot]] = None,
        row_limit: Optional[int] = None,
        max_batch_size: Optional[int] = None,
    ) -> Union[
        ResultIterator[pd.DataFrame],
        ResultIterator[pa.RecordBatch],
        ResultIterator[dict],
    ]:
        """Execute the TimeStream producing an iterator over the results.

        Args:
            kind: The kind of iterator to produce. Defaults to `pandas`.
            mode: The execution mode to use. Defaults to `'once'` to produce the results
              from the currently available data. Use `'live'` to start a standing query
              that continues to process new data until stopped.
            results: The results to produce. Defaults to `Histroy()` producing all points.
            row_limit: The maximum number of rows to return. Defaults to `None` for no limit.
            max_batch_size: The maximum number of rows to return in each batch.
              Defaults to `None` for no limit.

        Returns:
            Iterator over data of the corresponding kind. The `QueryIterator` allows
            cancelling the query or materialization as well as iterating.

        See Also:
            - :func:`write`: To write the results directly to a
              :class:`Destination<kaskada.destinations.Destination>`.
        """
        execution = self._execute(
            results, mode=mode, row_limit=row_limit, max_batch_size=max_batch_size
        )
        if kind == "pandas":
            return ResultIterator(execution, lambda table: iter((table.to_pandas(),)))
        elif kind == "pyarrow":
            return ResultIterator(execution, lambda table: table.to_batches())
        elif kind == "row":
            return ResultIterator(execution, lambda table: iter(table.to_pylist()))

        raise AssertionError(f"Unhandled kind {kind}")

    def _execute(
        self,
        results: Optional[Union[kd.results.History, kd.results.Snapshot]],
        *,
        row_limit: Optional[int] = None,
        max_batch_size: Optional[int] = None,
        mode: Literal["once", "live"] = "once",
    ) -> _ffi.Execution:
        """Execute the timestream and return the FFI handle for the execution."""
        expr = self
        if not pa.types.is_struct(self.data_type):
            # The execution engine requires a struct, so wrap this in a record.
            expr = record({"result": self})

        options = _ExecutionOptions(
            row_limit=row_limit,
            max_batch_size=max_batch_size,
            materialize=mode == "live",
        )

        if results is None:
            results = kd.results.History()

        if isinstance(results, kd.results.History):
            options.results = "history"
            if results.since is not None:
                options.changed_since = int(results.since.timestamp())
            if results.until is not None:
                options.final_at = int(results.until.timestamp())
        elif isinstance(results, kd.results.Snapshot):
            options.results = "snapshot"
            if results.changed_since is not None:
                options.changed_since = int(results.changed_since.timestamp())
            if results.at is not None:
                options.final_at = int(results.at.timestamp())
        else:
            raise AssertionError(f"Unhandled results type {results!r}")
        return expr._ffi_expr.execute(options)


def _aggregation(
    op: str,
    input: Timestream,
    window: Optional[kd.windows.Window],
    *args: Union[Timestream, LiteralValue],
) -> Timestream:
    """Return the aggregation `op` with the given `input`, `window` and `args`.

    Args:
    op: The operation to create.
    input: The input to the aggregation.
    window: The window to use for the aggregation.
    *args: Additional arguments to provide after `input` and before the flattened window.

    Raises:
        NotImplementedError: If the window is not a known type.
    """
    if window is None:
        return Timestream._call(op, input, *args, None, None)
    elif isinstance(window, kd.windows.Since):
        predicate = window.predicate
        if callable(predicate):
            predicate = predicate(input)
        return Timestream._call(op, input, *args, predicate, None)
    elif isinstance(window, kd.windows.Sliding):
        predicate = window.predicate
        if callable(predicate):
            predicate = predicate(input)
        return Timestream._call(op, input, *args, predicate, window.duration)
    elif isinstance(window, kd.windows.Trailing):
        if op != "collect":
            raise NotImplementedError(
                f"Aggregation '{op} does not support trailing windows"
            )

        trailing_ns = int(window.duration.total_seconds() * 1e9)

        # Create the shifted-forward input
        input_shift = input.shift_by(window.duration)

        # Merge, then extract just the input.
        #
        # Note: Assumes the "input" is discrete. Can probably
        # use a transform to make it discrete (eg., `input.filter(True)`)
        # or a special function to do that.
        #
        # HACK: This places an extra null row in the input to `collect`
        # which allows us to "clear" the window when the appropriate
        # `duration` has passed with no "real" inputs.
        merged_input = record({"input": input, "shift": input_shift}).col("input")
        return Timestream._call("collect", merged_input, *args, None, trailing_ns)
    else:
        raise NotImplementedError(f"Unknown window type {window!r}")


def record(fields: Mapping[str, Arg]) -> Timestream:
    """Return a record Timestream from the given fields.

    Args:
        fields: The fields to include in the record.

    See Also:
        Timestream.record: Method for creating a record from fields computed from
          a timestream.
    """
    import itertools

    args: List[Arg] = list(itertools.chain(*fields.items()))
    return Timestream._call("record", *args)
