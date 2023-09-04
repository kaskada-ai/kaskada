from __future__ import annotations

import sys
import warnings
from datetime import datetime, timedelta
from typing import Callable, List, Literal, Mapping, Optional, Sequence, Union

import kaskada as kd
import kaskada._ffi as _ffi
import pyarrow as pa
from typing_extensions import TypeAlias

from .._execution import _ExecutionOptions


class Timestream(object):
    """A `Timestream` represents a computation producing a Timestream."""

    _ffi_expr: _ffi.Expr

    def __init__(self, ffi: _ffi.Expr) -> None:
        """Create a new expression."""
        self._ffi_expr = ffi

    @staticmethod
    def _literal(value: LiteralValue, session: _ffi.Session) -> Timestream:
        """Construct a Timestream for a literal value."""
        if isinstance(value, timedelta):
            raise TypeError("Cannot create a literal Timestream from a timedelta")
        elif isinstance(value, datetime):
            raise TypeError("Cannot create a literal Timestream from a datetime")
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
        return expr._ffi_expr.execute(options)


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
