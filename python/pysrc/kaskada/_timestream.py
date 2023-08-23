"""Defines classes representing Kaskada expressions."""

from __future__ import annotations

import sys
import warnings
from datetime import datetime
from datetime import timedelta
from typing import Callable
from typing import List
from typing import Mapping
from typing import Optional
from typing import Sequence
from typing import Tuple
from typing import Union
from typing import final

import kaskada as kd
import kaskada._ffi as _ffi
import pandas as pd
import pyarrow as pa
from typing_extensions import TypeAlias

from ._execution import ExecutionOptions
from ._result import Result


#: A literal value that can be used as an argument to a Timestream operation.
Literal: TypeAlias = Optional[Union[int, str, float, bool, timedelta, datetime]]

#: A Timestream or literal which can be used as an argument to a Timestream operation.
Arg: TypeAlias = Union["Timestream", Literal]


def _augment_error(
    args: Sequence[Union[Timestream, Literal]], e: Exception
) -> Exception:
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


class Timestream(object):
    """A `Timestream` represents a computation producing a Timestream."""

    _ffi_expr: _ffi.Expr

    def __init__(self, ffi: _ffi.Expr) -> None:
        """Create a new expression."""
        self._ffi_expr = ffi

    @staticmethod
    def _literal(value: Literal, session: _ffi.Session) -> Timestream:
        """Construct a Timestream for a literal value."""
        if isinstance(value, timedelta):
            raise TypeError("Cannot create a literal Timestream from a timedelta")
        elif isinstance(value, datetime):
            raise TypeError("Cannot create a literal Timestream from a datetime")
        return Timestream(_ffi.Expr.literal(session, value))

    @staticmethod
    def _call(
        func: str,
        *args: Union[Timestream, Literal],
        session: Optional[_ffi.Session] = None,
    ) -> Timestream:
        """
        Construct a new Timestream by calling the given function.

        Parameters
        ----------
        func : str
            Name of the function to apply.
        *args : Timestream | int | str | float | bool | None
            List of arguments to the expression.
        session : FFI Session
            FFI Session to create the expression in.
            If unspecified, will infer from the arguments.
            Will fail if all arguments are literals and the session is not provided.

        Returns
        -------
        Timestream
            Timestream representing the result of the function applied to the arguments.

        Raises
        ------
        # noqa: DAR401 _augment_error
        TypeError
            If the argument types are invalid for the given function.
        ValueError
            If the argument values are invalid for the given function.
        """
        if session is None:
            session = next(
                arg._ffi_expr.session() for arg in args if isinstance(arg, Timestream)
            )

        def make_arg(arg: Union[Timestream, Literal]) -> _ffi.Expr:
            if isinstance(arg, Timestream):
                return arg._ffi_expr
            else:
                return Timestream._literal(arg, session)._ffi_expr

        ffi_args = [make_arg(arg) for arg in args]
        try:
            return Timestream(
                # TODO: FRAZ - so I need a `call` that can take the udf
                _ffi.Expr.call(session=session, operation=func, args=ffi_args)
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
        *args: Union[Timestream, Literal],
        **kwargs: Union[Timestream, Literal],
    ) -> Timestream:
        """
        Apply chainable functions that produce Timestreams.

        Parameters
        ----------
        func : Callable[..., Timestream] | Tuple[Callable[..., Timestream], str]
            Function to apply to this Timestream.  Alternatively a `(func,
            keyword)` tuple where `keyword` is a string indicating the keyword
            of `func` that expects the Timestream.
        args : iterable, optional
            Positional arguments passed into ``func``.
        kwargs : mapping, optional
            A dictionary of keyword arguments passed into ``func``.

        Returns
        -------
        Timestream
            The result of applying `func` to the arguments.

        Raises
        ------
        ValueError
            When using `self` with a specific `keyword` if the `keyword` also
            appears on in the `kwargs`.

        Notes
        -----
        Use ``.pipe`` when chaining together functions that expect Timestreams.

        Examples
        --------
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

    def add(self, rhs: Union[Timestream, Literal]) -> Timestream:
        """
        Create a Timestream adding this and `rhs`.

        Parameters
        ----------
        rhs : Union[Timestream, Literal]
            The Timestream or literal value to add to this.

        Returns
        -------
        Timestream
            The Timestream resulting from `self + rhs`.

        Notes
        -----
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
            return Timestream._call("add_time", seconds, self)
        else:
            return Timestream._call("add", self, rhs)

    def __add__(self, rhs: Union[Timestream, Literal]) -> Timestream:
        """Implement `self + rhs`."""
        return self.add(rhs)

    def __radd__(self, lhs: Union[Timestream, Literal]) -> Timestream:
        """Implement `lhs + self`."""
        if not isinstance(lhs, Timestream):
            lhs = Timestream._literal(lhs, self._ffi_expr.session())
        return lhs.add(self)

    def ceil(self) -> Timestream:
        """
        Create a Timestream for the number rounded up to the next largest integer.

        Returns
        -------
        Timestream
            The Timestream resulting from the numeric ceiling of this.
        """
        return Timestream._call("ceil", self)

    def clamp(
        self,
        min: Union[Timestream, Literal, None] = None,
        max: Union[Timestream, Literal, None] = None,
    ) -> Timestream:
        """
        Create a Timestream clamped between the bounds of min and max.

        Parameters
        ----------
        min : Union[Timestream, Literal, None]
            The literal value to set as the lower bound
        max : Union[Timestream, Literal, None]
            The literal value to set as the upper bound

        Returns
        -------
        Timestream
            The Timestream resulting from the clamped bounds between min and max.
        """
        return Timestream._call("clamp", self, min, max)

    def sub(self, rhs: Union[Timestream, Literal]) -> Timestream:
        """
        Create a Timestream substracting `rhs` from this.

        Parameters
        ----------
        rhs : Union[Timestream, Literal]
            The Timestream or literal value to subtract from this.

        Returns
        -------
        Timestream
            The Timestream resulting from `self - rhs`.

        Notes
        -----
        You can also write `a.sub(b)` as `a - b`.
        """
        return Timestream._call("sub", self, rhs)

    def __sub__(self, rhs: Union[Timestream, Literal]) -> Timestream:
        """Implement `self - rhs`."""
        return self.sub(rhs)

    def __rsub__(self, lhs: Union[Timestream, Literal]) -> Timestream:
        """Implement `lhs - self`."""
        if not isinstance(lhs, Timestream):
            lhs = Timestream._literal(lhs, self._ffi_expr.session())
        return lhs.sub(self)

    def exp(self) -> Timestream:
        """
        Create a Timestream raising `e` to the power of this.

        Returns
        -------
        Timestream
            The Timestream resulting from `e^this`.
        """
        return Timestream._call("exp", self)

    def floor(self) -> Timestream:
        """
        Create a Timestream of the values rounded down to the nearest integer.

        Returns
        -------
        Timestream
            The Timestream resulting from the numeric floor of this.
        """
        return Timestream._call("floor", self)

    def mul(self, rhs: Union[Timestream, Literal]) -> Timestream:
        """
        Create a Timestream multiplying this and `rhs`.

        Parameters
        ----------
        rhs : Union[Timestream, Literal]
            The Timestream or literal value to multiply with this.

        Returns
        -------
        Timestream
            The Timestream resulting from `self * rhs`.

        Notes
        -----
        You can also write `a.mul(b)` as `a * b`.
        """
        return Timestream._call("mul", self, rhs)

    def __mul__(self, rhs: Union[Timestream, Literal]) -> Timestream:
        """Implement `self * rhs`."""
        return self.mul(rhs)

    def __rmul__(self, lhs: Union[Timestream, Literal]) -> Timestream:
        """Implement `lhs * self`."""
        if not isinstance(lhs, Timestream):
            lhs = Timestream._literal(lhs, self._ffi_expr.session())
        return lhs.mul(self)

    def powf(self, power: Union[Timestream, Literal]) -> Timestream:
        """
        Create a Timestream raising `this` to the power of `power`.

        Parameters
        ----------
        power : Union[Timestream, Literal]
            The Timestream or literal value to raise this by.

        Returns
        -------
            Timestream: The Timestream resulting from `self ^ power`.
        """
        return Timestream._call("powf", self, power)

    def div(self, divisor: Union[Timestream, Literal]) -> Timestream:
        """
        Create a Timestream by dividing this and `divisor`.

        Parameters
        ----------
        divisor : Union[Timestream, Literal]
            The Timestream or literal value to divide this by.

        Returns
        -------
        Timestream
            The Timestream resulting from `self / divisor`.

        Notes
        -----
        You can also write `a.div(b)` as `a / b`.
        """
        return Timestream._call("div", self, divisor)

    def __truediv__(self, divisor: Union[Timestream, Literal]) -> Timestream:
        """Implement `self / divisor`."""
        return self.div(divisor)

    def __rtruediv__(self, dividend: Union[Timestream, Literal]) -> Timestream:
        """Implement `dividend / self`."""
        if not isinstance(dividend, Timestream):
            dividend = Timestream._literal(dividend, self._ffi_expr.session())
        return dividend.div(self)

    def lt(self, rhs: Union[Timestream, Literal]) -> Timestream:
        """
        Create a Timestream that is true if this is less than `rhs`.

        Parameters
        ----------
        rhs : Union[Timestream, Literal]
            The Timestream or literal value to compare to.

        Returns
        -------
        Timestream
            The Timestream resulting from `self < rhs`.

        Notes
        -----
        You can also write `a.lt(b)` as `a < b`.
        """
        return Timestream._call("lt", self, rhs)

    def __lt__(self, rhs: Union[Timestream, Literal]) -> Timestream:
        """Implement `self < rhs`."""
        return self.lt(rhs)

    def le(self, rhs: Union[Timestream, Literal]) -> Timestream:
        """
        Create a Timestream that is true if this is less than or equal to `rhs`.

        Parameters
        ----------
        rhs : Union[Timestream, Literal]
            The Timestream or literal value to compare to.

        Returns
        -------
        Timestream
            The Timestream resulting from `self <= rhs`.

        Notes
        -----
        You can also write `a.le(b)` as `a <= b`.
        """
        return Timestream._call("lte", self, rhs)

    def __le__(self, rhs: Union[Timestream, Literal]) -> Timestream:
        """Implement `self <= rhs`."""
        return self.le(rhs)

    def gt(self, rhs: Union[Timestream, Literal]) -> Timestream:
        """
        Create a Timestream that is true if this is greater than `rhs`.

        Parameters
        ----------
        rhs : Union[Timestream, Literal]
            The Timestream or literal value to compare to.

        Returns
        -------
        Timestream
            The Timestream resulting from `self > rhs`.

        Notes
        -----
        You can also write `a.gt(b)` as `a > b`.
        """
        return Timestream._call("gt", self, rhs)

    def __gt__(self, rhs: Union[Timestream, Literal]) -> Timestream:
        """Implement `self > rhs`."""
        return self.gt(rhs)

    def ge(self, rhs: Union[Timestream, Literal]) -> Timestream:
        """
        Create a Timestream that is true if this is greater than or equal to `rhs`.

        Parameters
        ----------
        rhs : Union[Timestream, Literal]
            The Timestream or literal value to compare to.

        Returns
        -------
        Timestream
            The Timestream resulting from `self >= rhs`.

        Notes
        -----
        You can also write `a.ge(b)` as `a >= b`.
        """
        return Timestream._call("gte", self, rhs)

    def __ge__(self, rhs: Union[Timestream, Literal]) -> Timestream:
        """Implement `self >= rhs`."""
        return self.ge(rhs)

    def and_(self, rhs: Union[Timestream, Literal]) -> Timestream:
        """
        Create the logical conjunction of this Timestream and `rhs`.

        Parameters
        ----------
        rhs : Union[Timestream, Literal]
            The Timestream or literal value to conjoin with.

        Returns
        -------
        Timestream
            The Timestream resulting from `self and rhs`.
        """
        return Timestream._call("logical_and", self, rhs)

    def or_(self, rhs: Union[Timestream, Literal]) -> Timestream:
        """
        Create the logical disjunction of this Timestream and `rhs`.

        Parameters
        ----------
        rhs : Union[Timestream, Literal]
            The Timestream or literal value to disjoin with.

        Returns
        -------
        Timestream
            The Timestream resulting from `self or rhs`.
        """
        return Timestream._call("logical_or", self, rhs)

    def not_(self) -> Timestream:
        """
        Create the logical negation of this Timestream.

        Returns
        -------
        Timestream
            The Timestream resulting from `not self`.
        """
        return Timestream._call("not", self)

    def eq(self, other: Union[Timestream, Literal]) -> Timestream:
        """
        Create a Timestream that is true if this is equal to `other`.

        Parameters
        ----------
        other : Union[Timestream, Literal]
            The Timestream or literal value to compare to.

        Returns
        -------
        Timestream
            The Timestream indicating whether the `self` and `other` are equal.

        Note
        ----
        Equality is *not* available as `a == b`.
        """
        return Timestream._call("eq", self, other)

    def ne(self, other: Union[Timestream, Literal]) -> Timestream:
        """
        Create a Timestream that is true if this is not equal to `other`.

        Parameters
        ----------
        other : Union[Timestream, Literal]
            The Timestream or literal value to compare to.

        Returns
        -------
        Timestream
            The Timestream indicating whether `self` and `other` are not equal.

        Note
        ----
        Inequality is *not* available as `a != b`.
        """
        return Timestream._call("neq", self, other)

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

    def index(self, key: Union[Timestream, Literal]) -> Timestream:
        """
        Index into the elements of a Timestream.

        If the Timestream contains lists, the key should be an integer index.

        If the Timestream contains maps, the key should be the same type as the map keys.

        Parameters
        ----------
        key : Union[Timestream, Literal]
            The key to index into the expression.

        Raises
        ------
        TypeError
            When the Timestream is not a record, list, or map.

        Returns
        -------
        Timestream
            Timestream with the resulting value (or `null` if absent) at each point.

        Note
        ----
        Indexing may be written using the operator `self[key]` instead of `self.index(key)`.
        """
        data_type = self.data_type
        if isinstance(data_type, pa.MapType):
            return Timestream._call("get", key, self)
        elif isinstance(data_type, pa.ListType):
            return Timestream._call("index", key, self)
        else:
            raise TypeError(f"Cannot index into {data_type}")

    def __getitem__(self, key: Union[Timestream, Literal]) -> Timestream:
        """
        Index into a list or map Timestrem.

        Parameters
        ----------
        key : Union[Timestream, Literal]
            The key to index into the expression.

        Returns
        -------
        Timestream
            Timestream with the resulting value (or `null` if absent) at each point.

        See Also
        --------
        index
        """
        return self.index(key)

    def col(self, name: str) -> Timestream:
        """
        Access a named column or field of a Timestream.

        Parameters
        ----------
        name : str
            The name of the column or field to access.

        Returns
        -------
        Timestream
            Timestream with the resulting value (or `null` if absent) at each point.

        Raises
        ------
        TypeError
            When the Timestream is not a record.
        """
        data_type = self.data_type
        if isinstance(data_type, pa.StructType) or isinstance(data_type, pa.ListType):
            return Timestream._call("fieldref", self, name)
        else:
            raise TypeError(
                f"Cannot access column {name!r} of non-record type '{data_type}'"  # noqa : B907
            )

    def select(self, *args: str) -> Timestream:
        """
        Select the given fields from a Timestream of records.

        Parameters
        ----------
        args : list[str]
            List of field names to select.

        Returns
        -------
        Timestream
            Timestream with the same records limited to the specified fields.
        """
        return Timestream._call("select_fields", self, *args)

    def remove(self, *args: str) -> Timestream:
        """
        Remove the given fileds from a Timestream of records.

        Parameters
        ----------
        args : list[str]
            List of field names to exclude.

        Returns
        -------
        Timestream
            Timestream with the same records and the given fields excluded.
        """
        return Timestream._call("remove_fields", self, *args)

    def extend(
        self, fields: Mapping[str, Arg] | Callable[[Timestream], Mapping[str, Arg]]
    ) -> Timestream:
        """
        Extend this Timestream of records with additional fields.

        If a field exists in the base Timestream and the `fields`, the value
        from the `fields` will be taken.

        Parameters
        ----------
        fields : Mapping[str, Arg] | Callable[[Timestream], Mapping[str, Arg]]
            Fields to add to each record in the Timestream.

        Returns
        -------
        Timestream
            Timestream with the given fields added.
        """
        # This argument order is weird, and we shouldn't need to make a record
        # in order to do the extension.
        if callable(fields):
            fields = fields(self)
        extension = record(fields)
        return Timestream._call("extend_record", extension, self)

    def neg(self) -> Timestream:
        """
        Create a Timestream from the numeric negation of self.

        Returns
        -------
        Timestream
            Timestream of the numeric negation of self.
        """
        return Timestream._call("neg", self)

    def is_null(self) -> Timestream:
        """
        Create a boolean Timestream containing `true` when self is `null`.

        Returns
        -------
        Timestream
            Timestream with `true` when self is `null` and `false` when it isn't.
        """
        return self.is_not_null().not_()

    def is_not_null(self) -> Timestream:
        """
        Create a boolean Timestream containing `true` when self is not `null`.

        Returns
        -------
        Timestream
            Timestream with `true` when self is not `null` and `false` when it is.
        """
        return Timestream._call("is_valid", self)

    def filter(self, condition: Timestream) -> Timestream:
        """
        Create a Timestream containing only the points where `condition` is `true`.

        Parameters
        ----------
        condition : Timestream
            The condition to filter on.

        Returns
        -------
        Timestream
            Timestream containing `self` where `condition` is `true`.
        """
        return Timestream._call("when", condition, self)

    def collect(
        self,
        *,
        max: Optional[int],
        min: Optional[int] = 0,
        window: Optional[kd.windows.Window] = None,
    ) -> Timestream:
        """
        Create a Timestream collecting up to the last `max` values in the `window`.

        Collects the values for each key separately.

        Parameters
        ----------
        max : Optional[int]
            The maximum number of values to collect.
            If `None` all values are collected.
        min: Optional[int]
            The minimum number of values to collect before
            producing a value. Defaults to 0.
        window : Optional[Window]
            The window to use for the aggregation.
            If not specified, the entire Timestream is used.

        Returns
        -------
        Timestream
            Timestream containing the collected list at each point.
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
        """
        Create a Timestream containing the time of each point.

        Returns
        -------
        Timestream
            Timestream containing the time of each point.
        """
        return Timestream._call("time_of", self)

    def lag(self, n: int) -> Timestream:
        """
        Create a Timestream containing the value `n` points before each point.

        Parameters
        ----------
        n : int
            The number of points to lag by.

        Returns
        -------
        Timestream
            Timestream containing the value `n` points before each point.
        """
        # hack to support structs/lists (as collect supports lists)
        return self.collect(max=n + 1, min=n + 1)[0]

    def if_(self, condition: Union[Timestream, Literal]) -> Timestream:
        """
        Return `self` where `condition` is `true`, or `null` otherwise.

        Parameters
        ----------
        condition : Union[Timestream, Literal]
            The condition to check.

        Returns
        -------
        Timestream
            Timestream containing the value of `self` where `condition` is `true`, or
            `null` otherwise.
        """
        return Timestream._call("if", condition, self)

    def null_if(self, condition: Union[Timestream, Literal]) -> Timestream:
        """
        Return `self` where `condition` is `false`, or `null` otherwise.

        Parameters
        ----------
        condition : Union[Timestream, Literal]
            The condition to check.

        Returns
        -------
        Timestream
            Timestream containing the value of `self` where `condition` is `false`, or
            `null` otherwise.
        """
        return Timestream._call("null_if", condition, self)

    def length(self) -> Timestream:
        """
        Create a Timestream containing the length of `self`.

        Returns
        -------
        Timestream
            Timestream containing the length of `self`.

        Raises
        ------
        TypeError
            When the Timestream is not a string or list.
        """
        if self.data_type.equals(pa.string()):
            return Timestream._call("len", self)
        elif isinstance(self.data_type, pa.ListType):
            return Timestream._call("list_len", self)
        else:
            raise TypeError(f"length not supported for {self.data_type}")

    def with_key(self, key: Timestream, grouping: Optional[str] = None) -> Timestream:
        """
        Create a Timestream with a new grouping by `key`.

        Parameters
        ----------
        key : Timestream
            The new key to use for the grouping.
        grouping : Optional[str]
            A string literal naming the new grouping. If no `grouping` is specified,
            one will be computed from the type of the `key`.

        Returns
        -------
        Timestream
            Timestream with a new grouping by `key`.
        """
        return Timestream._call("with_key", key, self, grouping)

    def lookup(self, key: Union[Timestream, Literal]) -> Timestream:
        """
        Lookup the value of `self` for each `key` at the times in `key`.

        For each non-`null` point in the `key` timestream, returns the value
        from `self` at that time and associated with that `key`. Returns `null`
        if the `key` is `null` or if there is no `value` computed for that key
        at the corresponding time.

        Parameters
        ----------
        key : Union[Timestream, Literal]
            The foreign key to lookup.
            This must match the type of the keys in `self`.

        Returns
        -------
        Timestream
            Timestream containing the lookup join between the `key` and `self`.
        """
        return Timestream._call("lookup", key, self)

    def coalesce(
        self, arg: Union[Timestream, Literal], *args: Union[Timestream, Literal]
    ) -> Timestream:
        """
        Create a Timestream for returning the first non-null value or null if all values are null.

        Parameters
        ----------
        arg : Union[Timestream, Literal]
            The next value to be coalesced (required).
        args : Union[Timestream, Literal]
            Additional values to be coalesced (optional).

        Returns
        -------
        Timestream
            Timestream containing the first non-null value from that row.
            If all values are null, then returns null.
        """
        return Timestream._call("coalesce", self, arg, *args)

    def shift_to(self, time: Union[Timestream, datetime]) -> Timestream:
        """
        Create a Timestream shifting each point forward to `time`.

        If multiple values are shifted to the same time, they will be emitted in
        the order in which they originally occurred.

        Parameters
        ----------
        time : Union[Timestream, datetime]
            The time to shift to.
            This must be a datetime or a Timestream of timestamp_ns.

        Returns
        -------
        Timestream
            Timestream containing the shifted points.

        Raises
        ------
        NotImplementedError
            When `time` is a datetime (shift_to literal not yet implemented).
        """
        if isinstance(time, datetime):
            # session = self._ffi_expr.session()
            # time_ns = time.timestamp() * 1e9
            # time_ns = Timestream._literal(time_ns, session=session)
            # time_ns = Timestream.cast(time_ns, pa.timestamp('ns'))
            # return Timestream._call("shift_to", time_ns, self)
            raise NotImplementedError("shift_to with datetime literal unsupported")
        else:
            return Timestream._call("shift_to", time, self)

    def shift_by(self, delta: Union[Timestream, timedelta]) -> Timestream:
        """
        Create a Timestream shifting each point forward by the `delta`.

        If multiple values are shifted to the same time, they will be emitted in
        the order in which they originally occurred.

        Parameters
        ----------
        delta : Union[Timestream, timedelta]
            The delta to shift the point forward by.

        Returns
        -------
        Timestream
            Timestream containing the shifted points.
        """
        if isinstance(delta, timedelta):
            session = self._ffi_expr.session()
            seconds = Timestream._call(
                "seconds", int(delta.total_seconds()), session=session
            )
            return Timestream._call("shift_by", seconds, self)
        else:
            return Timestream._call("shift_by", delta, self)

    def shift_until(self, predicate: Timestream) -> Timestream:
        """
        Shift points from `self` forward to the next time `predicate` is true.

        Note that if the `predicate` evaluates to true at the same time as `self`,
        the point will be emitted at that time.

        If multiple values are shifted to the same time, they will be emitted in
        the order in which they originally occurred.

        Parameters
        ----------
        predicate : Timestream
            The predicate to determine whether to emit shifted rows.

        Returns
        -------
        Timestream
            Timestream containing the shifted points.
        """
        return Timestream._call("shift_until", predicate, self)

    def sum(self, *, window: Optional[kd.windows.Window] = None) -> Timestream:
        """
        Create a Timestream summing the values in the `window`.

        Computes the sum for each key separately.

        Parameters
        ----------
        window : Optional[Window]
            The window to use for the aggregation.
            If not specified, the entire Timestream is used.

        Returns
        -------
        Timestream
            Timestream containing the sum up to and including each point.
        """
        return _aggregation("sum", self, window)

    def first(self, *, window: Optional[kd.windows.Window] = None) -> Timestream:
        """
        Create a Timestream containing the first value in the `window`.

        Computed for each key separately.

        Parameters
        ----------
        window : Optional[Window]
            The window to use for the aggregation.
            If not specified, the entire Timestream is used.

        Returns
        -------
        Timestream
            Timestream containing the first value for the key in the window for
            each point.
        """
        return _aggregation("first", self, window)

    def last(self, window: Optional[kd.windows.Window] = None) -> Timestream:
        """
        Create a Timestream containing the last value in the `window`.

        Computed for each key separately.

        Parameters
        ----------
        window : Optional[Window]
            The window to use for the aggregation.
            If not specified, the entire Timestream is used.

        Returns
        -------
        Timestream
            Timestream containing the last value for the key in the window for
            each point.
        """
        return _aggregation("last", self, window)

    def count(self, window: Optional[kd.windows.Window] = None) -> Timestream:
        """
        Create a Timestream containing the count value in the `window`.

        Computed for each key separately.

        Parameters
        ----------
        window : Optional[Window]
            The window to use for the aggregation.
            If not specified, the entire Timestream is used.

        Returns
        -------
        Timestream
            Timestream containing the count value for the key in the window for
            each point.
        """
        return _aggregation("count", self, window)

    def count_if(self, window: Optional[kd.windows.Window] = None) -> Timestream:
        """
        Create a Timestream containing the count of `true` values in `window`.

        Computed for each key separately.

        Parameters
        ----------
        window : Optional[Window]
            The window to use for the aggregation.
            If not specified, the entire Timestream is used.

        Returns
        -------
        Timestream
            Timestream containing the count value if true for the key in the window for
            each point.
        """
        return _aggregation("count_if", self, window)

    def max(self, window: Optional[kd.windows.Window] = None) -> Timestream:
        """
        Create a Timestream containing the max value in the `window`.

        Computed for each key separately.

        Parameters
        ----------
        window : Optional[Window]
            The window to use for the aggregation.
            If not specified, the entire Timestream is used.

        Returns
        -------
        Timestream
            Timestream containing the max value for the key in the window for
            each point.
        """
        return _aggregation("max", self, window)

    def min(self, window: Optional[kd.windows.Window] = None) -> Timestream:
        """
        Create a Timestream containing the min value in the `window`.

        Computed for each key separately.

        Parameters
        ----------
        window : Optional[Window]
            The window to use for the aggregation.
            If not specified, the entire Timestream is used.

        Returns
        -------
        Timestream
            Timestream containing the min value for the key in the window for
            each point.
        """
        return _aggregation("min", self, window)

    def mean(self, window: Optional[kd.windows.Window] = None) -> Timestream:
        """
        Create a Timestream containing the mean value in the `window`.

        Computed for each key separately.

        Parameters
        ----------
        window : Optional[Window]
            The window to use for the aggregation.
            If not specified, the entire Timestream is used.

        Returns
        -------
        Timestream
            Timestream containing the mean value for the key in the window for
            each point.
        """
        return _aggregation("mean", self, window)

    def stddev(self, window: Optional[kd.windows.Window] = None) -> Timestream:
        """
        Create a Timestream containing the standard deviation in the `window`.

        Computed for each key separately.

        Parameters
        ----------
        window : Optional[Window]
            The window to use for the aggregation.
            If not specified, the entire Timestream is used.

        Returns
        -------
        Timestream
            Timestream containing the standard deviation for the key in the window for
            each point.
        """
        return _aggregation("stddev", self, window)

    def variance(self, window: Optional[kd.windows.Window] = None) -> Timestream:
        """
        Create a Timestream containing the variance in the `window`.

        Computed for each key separately.

        Parameters
        ----------
        window : Optional[Window]
            The window to use for the aggregation.
            If not specified, the entire Timestream is used.

        Returns
        -------
        Timestream
            Timestream containing the variance for the key in the window for
            each point.
        """
        return _aggregation("variance", self, window)

    def cast(self, data_type: pa.DataType) -> Timestream:
        """
        Cast the type of this Timestream to the given data type.

        Parameters
        ----------
        data_type : pa.DataType
            The data type to cast to.

        Returns
        -------
        Timestream
            Timestream with the given data type.
        """
        return Timestream(self._ffi_expr.cast(data_type))

    def else_(self, other: Timestream) -> Timestream:
        """
        Return `self` if not `null` otherwise `other`.

        Parameters
        ----------
        other : Timestream
            The Timestream to use if self is `null`.

        Returns
        -------
        Timestream
            Timestream containing the value of `self` not `null` otherwise `other`.
        """
        return Timestream._call("else", other, self)

    def seconds_since(self, time: Union[Timestream, Literal]) -> Timestream:
        """
        Return a Timestream containing seconds between `time` and `self`.

        Parameters
        ----------
        time : Union[Timestream, Literal]
            The time to compute the seconds since.

            This can be either a stream of timestamps or a datetime literal.
            If `time` is a Timestream, the result will contain the seconds
            from `self.time()` to `time.time()` for each point.

        Returns
        -------
        Timestream
            Timestream containing the number of seconds since `time`.

            If `self.time()` is greater than `time`, the result will be positive.
        """
        if isinstance(time, datetime):
            session = self._ffi_expr.session()
            nanos = Timestream._literal(time.timestamp() * 1e9, session=session)
            nanos = Timestream.cast(nanos, pa.timestamp("ns", None))
            return Timestream._call("seconds_between", nanos, self)
        else:
            return Timestream._call("seconds_between", time, self)

    def seconds_since_previous(self, n: int = 1) -> Timestream:
        """
        Return a Timestream containing seconds between `self` and the time `n` points ago.

        Parameters
        ----------
        n : int
            The number of points to look back. For example, `n=1` refers to
            the previous point.

            Defaults to 1 (the previous point).

        Returns
        -------
        Timestream
            Timestream containing the number of seconds since the time `n`
            points ago.
        """
        time_of_current = Timestream._call("time_of", self).cast(pa.int64())
        time_of_previous = Timestream._call("time_of", self).lag(n).cast(pa.int64())

        # `time_of` returns nanoseconds, so divide to get seconds
        return time_of_current.sub(time_of_previous).div(1e9).cast(pa.duration("s"))

    def flatten(self) -> Timestream:
        """Flatten a list of lists to a list of values."""
        return Timestream._call("flatten", self)

    def union(self, other: Timestream) -> Timestream:
        """
        Union the lists in this timestream with the lists in the other Timestream.

        This correspons to a pair-wise union within each row of the timestreams.

        Parameters
        ----------
        other : Timestream
            The Timestream of lists to union with.

        Returns
        -------
        Timestream
            Timestream containing the union of the lists.
        """
        return Timestream._call("union", self, other)

    def record(self, fields: Callable[[Timestream], Mapping[str, Arg]]) -> Timestream:
        """
        Create a record Timestream from fields computed from this timestream.

        Parameters
        ----------
        fields : Callable[[Timestream], Mapping[str, Arg]]
            The fields to include in the record.

        Returns
        -------
        Timestream
            Timestream containing records with the given fields.

        See Also
        --------
        kaskada.record: Function for creating a record from one or more
        timestreams.
        """
        return record(fields(self))

    def preview(self, limit: int = 100) -> pd.DataFrame:
        """
        Return the first N rows of the result as a Pandas DataFrame.

        This makes it easy to preview the content of the Timestream.

        Parameters
        ----------
        limit : int
            Maximum number of rows to print.

        Returns
        -------
        pd.DataFrame
            The Pandas DataFrame containing the first `limit` points.
        """
        return self.run(row_limit=limit).to_pandas()

    def run(
        self,
        row_limit: Optional[int] = None,
        max_batch_size: Optional[int] = None,
        materialize: bool = False,
    ) -> Result:
        """
        Run the Timestream once.

        Parameters
        ----------
        row_limit : Optional[int]
            The maximum number of rows to return.
            If not specified all rows are returned.

        max_batch_size : Optional[int]
            The maximum number of rows to return in each batch.
            If not specified the default is used.

        materialize : bool
            If true, the execution will be a continuous materialization.

        Returns
        -------
        Result
            The `Result` object to use for accessing the results.
        """
        expr = self
        if not pa.types.is_struct(self.data_type):
            # The execution engine requires a struct, so wrap this in a record.
            expr = record({"result": self})
        options = ExecutionOptions(
            row_limit=row_limit, max_batch_size=max_batch_size, materialize=materialize
        )
        execution = expr._ffi_expr.execute(options)
        return Result(execution)


def _aggregation(
    op: str,
    input: Timestream,
    window: Optional[kd.windows.Window],
    *args: Union[Timestream, Literal],
) -> Timestream:
    """
    Create the aggregation `op` with the given `input`, `window` and `args`.

    Parameters
    ----------
    op : str
        The operation to create.
    input : Timestream
        The input to the aggregation.
    window : Optional[Window]
        The window to use for the aggregation.
    *args : Union[Timestream, Literal]
        Additional arguments to provide after `input` and before the flattened window.

    Returns
    -------
    Timestream
        The resulting Timestream.

    Raises
    ------
    NotImplementedError
        If the window is not a known type.
    """
    if window is None:
        return Timestream._call(op, input, *args, None, None)
    elif isinstance(window, kd.windows.Since):
        return Timestream._call(op, input, *args, window.predicate, None)
    elif isinstance(window, kd.windows.Sliding):
        return Timestream._call(op, input, *args, window.predicate, window.duration)
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
    """
    Create a record Timestream from the given fields.

    Parameters
    ----------
    fields : dict[str, Timestream]
        The fields to include in the record.

    Returns
    -------
    Timestream
        Timestream containing records with the given fields.

    See Also
    --------
    Timestream.record: Method for creating a record from fields computed from
    a timestream.
    """
    import itertools

    args: List[Arg] = list(itertools.chain(*fields.items()))
    return Timestream._call("record", *args)
