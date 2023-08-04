"""Defines classes representing Kaskada expressions."""

import sys
from typing import Callable
from typing import Dict
from typing import Optional
from typing import Sequence
from typing import Tuple
from typing import Union
from typing import final

import pandas as pd
import pyarrow as pa
from ._execution import ExecutionOptions
import sparrow_py as kt
import sparrow_py._ffi as _ffi


#: The type of arguments to expressions.
Arg = Union["Expr", int, str, float, None]


def _augment_error(args: Sequence[Arg], e: Exception) -> Exception:
    """Augment an error with information about the arguments."""
    if sys.version_info >= (3, 11):
        # If we can add notes to the exception, indicate the types.
        # This works in Python >=3.11
        for n, arg in enumerate(args):
            if isinstance(arg, Expr):
                e.add_note(f"Arg[{n}]: Expr of type {arg.data_type}")
            else:
                e.add_note(f"Arg[{n}]: Literal {arg} ({type(arg)})")
    return e


class Expr(object):
    """A Kaskada expression."""

    _ffi_expr: _ffi.Expr

    def __init__(self, ffi: _ffi.Expr) -> None:
        """Create a new expression."""
        self._ffi_expr = ffi

    @staticmethod
    def call(name: str, *args: Arg) -> "Expr":
        """
        Construct a new expression calling the given name.

        Parameters
        ----------
        name : str
            Name of the operation to apply.
        args : list[Expr]
            List of arguments to the expression.

        Returns
        -------
        Expression representing the given operation applied to the arguments.

        Raises
        ------
        # noqa: DAR401 _augment_error
        TypeError
            If the argument types are invalid for the given function.
        ValueError
            If the argument values are invalid for the given function.
        """
        ffi_args = [arg._ffi_expr if isinstance(arg, Expr) else arg for arg in args]
        session = next(arg._ffi_expr.session() for arg in args if isinstance(arg, Expr))
        try:
            return Expr(_ffi.Expr(session=session, operation=name, args=ffi_args))
        except TypeError as e:
            # noqa: DAR401
            raise _augment_error(args, TypeError(str(e))) from e
        except ValueError as e:
            raise _augment_error(args, ValueError(str(e))) from e

    @property
    def data_type(self) -> pa.DataType:
        """Return the data type produced by this expression."""
        return self._ffi_expr.data_type()

    def __eq__(self, other: object) -> bool:
        """Return true if the expressions are equivalent."""
        if not isinstance(other, Expr):
            return False
        return self._ffi_expr.equivalent(other._ffi_expr)

    @final
    def pipe(
        self,
        func: Union[Callable[..., "Expr"], Tuple[Callable[..., "Expr"], str]],
        *args: Arg,
        **kwargs: Arg,
    ) -> "Expr":
        """
        Apply chainable functions that expect expressions.

        Parameters
        ----------
        func : function
            Function to apply to this expression.
            ``args``, and ``kwargs`` are passed into ``func``.
            Alternatively a ``(callable, keyword)`` tuple where
            ``keyword`` is a string indicating the keyword of
            ``callable`` that expects the expression.
        args : iterable, optional
            Positional arguments passed into ``func``.
        kwargs : mapping, optional
            A dictionary of keyword arguments passed into ``func``.

        Returns
        -------
        Expr
            The result of applying `func` to the arguments.

        Raises
        ------
        ValueError
            When using `self` with a specific `keyword` if the `keyword` also
            appears on in the `kwargs`.

        Notes
        -----
        Use ``.pipe`` when chaining together functions that expect
        expressions.
        Instead of writing

        >>> func(g(h(df), arg1=a), arg2=b, arg3=c)  # doctest: +SKIP

        You can write

        >>> (df.pipe(h)
        ...    .pipe(g, arg1=a)
        ...    .pipe(func, arg2=b, arg3=c)
        ... )  # doctest: +SKIP

        If you have a function that takes the data as (say) the second
        argument, pass a tuple indicating which keyword expects the
        data. For example, suppose ``func`` takes its data as ``arg2``:

        >>> (df.pipe(h)
        ...    .pipe(g, arg1=a)
        ...    .pipe((func, 'arg2'), arg1=a, arg3=c)
        ...  )  # doctest: +SKIP
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

    def __add__(self, rhs: Arg) -> "Expr":
        """Add two expressions."""
        return Expr.call("add", self, rhs)

    def __radd__(self, lhs: Arg) -> "Expr":
        """Add two expressions."""
        return Expr.call("add", lhs, self)

    def __sub__(self, rhs: Arg) -> "Expr":
        """Subtract two expressions."""
        return Expr.call("sub", self, rhs)

    def __mul__(self, rhs: Arg) -> "Expr":
        """Multiple two expressions."""
        return Expr.call("mul", self, rhs)

    def __truediv__(self, rhs: Arg) -> "Expr":
        """Divide two expressions."""
        return Expr.call("div", self, rhs)

    def __lt__(self, rhs: Arg) -> "Expr":
        """Less than comparison."""
        return Expr.call("lt", self, rhs)

    def __le__(self, rhs: Arg) -> "Expr":
        """Less than or equal comparison."""
        return Expr.call("le", self, rhs)

    def __gt__(self, rhs: Arg) -> "Expr":
        """Greater than comparison."""
        return Expr.call("gt", self, rhs)

    def __ge__(self, rhs: Arg) -> "Expr":
        """Greater than or equal comparison."""
        return Expr.call("ge", self, rhs)

    def __and__(self, rhs: Arg) -> "Expr":
        """Logical and."""
        return Expr.call("and", self, rhs)

    def __or__(self, rhs: Arg) -> "Expr":
        """Logical or."""
        return Expr.call("or", self, rhs)

    def __getitem__(self, key: Arg) -> "Expr":
        """
        Index into an expression.

        If the expression is a struct, the key should be a string corresponding
        to a field.

        If the expression is a list, the key should be an integer index. If the
        expression is a map, the key should be the same type as the map keys.

        Parameters
        ----------
        key : Arg
            The key to index into the expression.

        Returns
        -------
        Expression accessing the given index.

        Raises
        ------
        TypeError
            When the expression is not a struct, list, or map.
        """
        data_type = self.data_type
        if isinstance(data_type, pa.StructType):
            return Expr.call("fieldref", self, key)
        elif isinstance(data_type, pa.MapType):
            return Expr.call("get_map", self, key)
        elif isinstance(data_type, pa.ListType):
            return Expr.call("get_list", self, key)
        else:
            raise TypeError(f"Cannot index into {data_type}")

    def eq(self, other: "Expr") -> "Expr":
        """Expression evaluating to true if self and other are equal."""
        return Expr.call("eq", self, other)

    def ne(self, other: "Expr") -> "Expr":
        """Expression evaluating to true if self and other are not equal."""
        return Expr.call("ne", self, other)

    def select(self, invert: bool = False, *args: str) -> "Expr":
        """
        Select the given fields from a struct.

        Parameters
        ----------
        invert : bool
            If false (default), select only the fields given.
            If true, select all fields except those given.
        args : list[str]
            List of field names to select (or remove).

        Returns
        -------
        Expression selecting (or excluding) the given fields.
        """
        if invert:
            return Expr.call("remove_fields", self, *args)
        else:
            return Expr.call("select_fields", self, *args)

    def extend(self, fields: Dict[str, "Expr"]) -> "Expr":
        """Extend this record with the additoonal fields."""
        # This argument order is weird, and we shouldn't need to make a record
        # in order to do the extension.
        extension = kt.record(fields)
        return Expr.call("extend_record", extension, self)

    def neg(self) -> "Expr":
        """Apply logical or numerical negation, depending on the type."""
        data_type = self.data_type
        if data_type == pa.bool_():
            return Expr.call("not", self)
        else:
            return Expr.call("neg", self)

    def is_null(self) -> "Expr":
        """Return a boolean expression indicating if the expression is null."""
        return self.is_not_null().neg()

    def is_not_null(self) -> "Expr":
        """Return a boolean expression indicating if the expression is not null."""
        return Expr.call("is_valid", self)

    def collect(
        self, max: Optional[int], window: Optional["kt.Window"] = None
    ) -> "Expr":
        """Return an expression collecting the last `max` values in the `window`."""
        return _aggregation("collect", self, window, max)

    def sum(self, window: Optional["kt.Window"] = None) -> "Expr":
        """Return the sum aggregation of the expression."""
        return _aggregation("sum", self, window)

    def first(self, window: Optional["kt.Window"] = None) -> "Expr":
        """Return the first aggregation of the expression."""
        return _aggregation("first", self, window)

    def last(self, window: Optional["kt.Window"] = None) -> "Expr":
        """Return the last aggregation of the expression."""
        return _aggregation("last", self, window)

    def show(self, limit: int = 100) -> None:
        """
        Print the first N rows of the result.

        This is intended for debugging purposes.

        Parameters
        ----------
        limit : int
            Maximum number of rows to print.
        """
        df = self.run(row_limit=limit)
        try:
            import ipython  # type: ignore

            ipython.display(df)
        except ImportError:
            print(df)

    def run(self, row_limit: Optional[int] = None) -> pd.DataFrame:
        """Run the expression."""
        options = ExecutionOptions(row_limit=row_limit)
        batches = self._ffi_expr.execute(options).collect_pyarrow()
        schema = batches[0].schema
        table = pa.Table.from_batches(batches, schema=schema)
        return table.to_pandas()

    def run_to_csv_string(self) -> str:
        """Execute the expression and return the CSV as a string."""
        # Convert PyArrow -> Pandas -> CSV, omitting the index (row).
        return self.run().to_csv(index=False)


def _aggregation(
    op: str, input: Expr, window: Optional["kt.Window"], *args: Optional[Arg]
) -> Expr:
    """
    Create the aggregation `op` with the given `input`, `window` and `args`.

    Parameters
    ----------
    op : str
        The operation to create.
    input : Expr
        The input to the expression.
    window : Optional[Window]
        The window to use for the aggregation.
    *args : Optional[Arg]
        Additional arguments to provide before `input` and the flattened window.

    Returns
    -------
    The resulting expression.

    Raises
    ------
    NotImplementedError
        If the window is not a known type.
    """
    # Note: things would be easier if we had a more normal order, which
    # we could do as part of "aligning" Sparrow signatures to the new direction.
    # However, `collect` currently has `collect(max, input, window)`, requiring
    # us to add the *args like so.
    if window is None:
        return Expr.call(op, *args, input, None, None)
    elif isinstance(window, kt.SinceWindow):
        return Expr.call(op, *args, input, window._predicate, None)
    elif isinstance(window, kt.SlidingWindow):
        return Expr.call(op, *args, input, window._predicate, window._duration)
    else:
        raise NotImplementedError(f"Unknown window type {window!r}")
