"""Defines classes representing Kaskada expressions."""

from __future__ import annotations

from datetime import timedelta
import sys
from typing import Callable
from typing import Dict
from typing import List
from typing import Optional
from typing import Sequence
from typing import Tuple
from typing import Union
from typing import final
from typing_extensions import TypeAlias

import pandas as pd
import pyarrow as pa
import sparrow_py as kt
import sparrow_py._ffi as _ffi

from ._execution import ExecutionOptions
from ._result import Result


Literal = Union[int, str, float, bool, None, timedelta]

def _augment_error(args: Sequence[Union[Timestream, Literal]], e: Exception) -> Exception:
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
    """
    A `Timestream` represents a computation producing a Timestream.
    """

    _ffi_expr: _ffi.Expr

    def __init__(self, ffi: _ffi.Expr) -> None:
        """Create a new expression."""
        self._ffi_expr = ffi

    @staticmethod
    def _call(func: str, *args: Union[Timestream, Literal], session: Optional[_ffi.Session] = None) -> Timestream:
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
        ffi_args = [
            arg._ffi_expr if isinstance(arg, Timestream) else arg for arg in args
        ]
        if session is None:
            session = next(
                arg._ffi_expr.session() for arg in args if isinstance(arg, Timestream)
            )
        try:
            return Timestream(_ffi.Expr(session=session, operation=func, args=ffi_args))
        except TypeError as e:
            # noqa: DAR401
            raise _augment_error(args, TypeError(str(e))) from e
        except ValueError as e:
            raise _augment_error(args, ValueError(str(e))) from e

    @property
    def data_type(self) -> pa.DataType:
        """The PyArrow type of values in this Timestream."""
        return self._ffi_expr.data_type()

    @final
    def pipe(
        self,
        func: Union[
            Callable[..., Timestream], Tuple[Callable[..., Timestream], str]
        ],
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

    def __add__(self, rhs: Union[Timestream, Literal]) -> Timestream:
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

        Raises
        ------
        ValueError
            If attempting to perform an addition that is not possible.
        """
        if isinstance(rhs, timedelta):
            # Right now, we can't convert a time delta directly to a scalar value (literal).
            # So we convert it to seconds and then add it.
            # Note that this loses precision if the timedelta has a fractional number of seconds,
            # and fail if the number of seconds exceeds an integer.
            session = self._ffi_expr.session()
            seconds = Timestream._call("seconds", int(rhs.total_seconds()), session = session)
            return Timestream._call("add_time", seconds, self)
        else:
            return Timestream._call("add", self, rhs)

    def __radd__(self, lhs: Union[Timestream, Literal]) -> Timestream:
        """
        Create a Timestream adding `lhs` and this.

        Parameters
        ----------
        lhs : Union[Timestream, Literal]
            The Timestream or literal value to add to this.

        Returns
        -------
        Timestream
            The Timestream resulting from `lhs + self`.
        """
        return Timestream._call("add", lhs, self)

    def __sub__(self, rhs: Union[Timestream, Literal]) -> Timestream:
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
        """
        return Timestream._call("sub", self, rhs)

    def __rsub__(self, lhs: Union[Timestream, Literal]) -> Timestream:
        """
        Create a Timestream substracting this from the `lhs`.

        Parameters
        ----------
        lhs : Union[Timestream, Literal]
            The Timestream or literal value to subtract this from.

        Returns
        -------
        Timestream
            The Timestream resulting from `lhs - self`.
        """
        return Timestream._call("sub", lhs, self)

    def __mul__(self, rhs: Union[Timestream, Literal]) -> Timestream:
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
        """
        return Timestream._call("mul", self, rhs)

    def __rmul__(self, lhs: Union[Timestream, Literal]) -> Timestream:
        """
        Create a Timestream multiplying `lhs` and this.

        Parameters
        ----------
        lhs : Union[Timestream, Literal]
            The Timestream or literal value to multiply with this.

        Returns
        -------
        Timestream
            The Timestream resulting from `lhs * self`.
        """
        return Timestream._call("mul", lhs, self)

    def __truediv__(self, divisor: Union[Timestream, Literal]) -> Timestream:
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
        """
        return Timestream._call("div", self, divisor)

    def __rtruediv__(self, dividend: Union[Timestream, Literal]) -> Timestream:
        """
        Create a Timestream by dividing this and `dividend`.

        Parameters
        ----------
        dividend : Union[Timestream, Literal]
            The Timestream or literal value to divide by this.

        Returns
        -------
        Timestream
            The Timestream resulting from `dividend / self`.
        """
        return Timestream._call("div", dividend, self)

    def __lt__(self, rhs: Union[Timestream, Literal]) -> Timestream:
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
        """
        return Timestream._call("lt", self, rhs)

    def __le__(self, rhs: Union[Timestream, Literal]) -> Timestream:
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
        """
        return Timestream._call("lte", self, rhs)

    def __gt__(self, rhs: Union[Timestream, Literal]) -> Timestream:
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
        """
        return Timestream._call("gt", self, rhs)

    def __ge__(self, rhs: Union[Timestream, Literal]) -> Timestream:
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
        """
        return Timestream._call("gte", self, rhs)

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
        """
        return Timestream._call("neq", self, other)

    def __getitem__(self, key: Union[Timestream, Literal]) -> Timestream:
        """
        Index into the elements of a Timestream.

        If the Timestream contains records, the key should be a string corresponding
        to a field.

        If the Timestream contains lists, the key should be an integer index.

        If the Timestream contains maps, the key should be the same type as the map keys.

        Parameters
        ----------
        key : Union[Timestream, Literal]
            The key to index into the expression.

        Returns
        -------
        Timestream
            Timestream with the resulting value (or `null` if absent) at each point.

        Raises
        ------
        TypeError
            When the Timestream is not a record, list, or map.
        """
        data_type = self.data_type
        if isinstance(data_type, pa.StructType):
            return Timestream._call("fieldref", self, key)
        elif isinstance(data_type, pa.MapType):
            return Timestream._call("get_map", self, key)
        elif isinstance(data_type, pa.ListType):
            return Timestream._call("get_list", self, key)
        else:
            raise TypeError(f"Cannot index into {data_type}")

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

    def extend(self, fields: Dict[str, Timestream]) -> Timestream:
        """
        Extend this Timestream of records with additional fields.

        If a field exists in the base Timestream and the `fields`, the value
        from the `fields` will be taken.

        Parameters
        ----------
        fields : dict[str, Timestream]
            Fields to add to each record in the Timestream.

        Returns
        -------
        Timestream
            Timestream with the given fields added.
        """
        # This argument order is weird, and we shouldn't need to make a record
        # in order to do the extension.
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
        return self.is_not_null().neg()

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
        self, max: Optional[int], min: Optional[int] = 0, window: Optional["kt.Window"] = None
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
        return _aggregation("collect", self, window, max, min)

    def time_of(self) -> Timestream:
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
        return Timestream._call("lag", n, self)

    def if_(self, condition: Union[Timestream, Literal]) -> Timestream:
        """
        Create a Timestream containing the value of `self` where `condition` is `true`,
        or `null` otherwise.

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
        Create a Timestream containing `self` where `condition` is `false`, or `null` otherwise.

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
        Create a Timestream containing the lookup join between the `key` and
        `self`.

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

    def sum(self, window: Optional["kt.Window"] = None) -> Timestream:
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

    def first(self, window: Optional["kt.Window"] = None) -> Timestream:
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

    def last(self, window: Optional["kt.Window"] = None) -> Timestream:
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
        options = ExecutionOptions(row_limit=row_limit, max_batch_size=max_batch_size, materialize=materialize)
        execution = expr._ffi_expr.execute(options)
        return Result(execution)


def _aggregation(
    op: str, input: Timestream, window: Optional["kt.Window"], *args: Union[Timestream, Literal]
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
    elif isinstance(window, kt.SinceWindow):
        return Timestream._call(op, input, *args, window.predicate, None)
    elif isinstance(window, kt.SlidingWindow):
        return Timestream._call(op, input, *args, window.predicate, window.duration)
    else:
        raise NotImplementedError(f"Unknown window type {window!r}")


def record(fields: Dict[str, Timestream]) -> Timestream:
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
    """
    import itertools

    args: List[Union[str, Timestream]] = list(itertools.chain(*fields.items()))
    return Timestream._call("record", *args)
