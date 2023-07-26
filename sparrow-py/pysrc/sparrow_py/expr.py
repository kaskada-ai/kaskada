"""Defines classes representing Kaskada expressions."""

import sys
from typing import Callable
from typing import Tuple
from typing import Union
from typing import final

from sparrow_py import ffi


class Expr(object):
    """A Kaskada expression."""

    ffi: ffi.Expr

    def __init__(self, name: str, *args: "Expr") -> None:
        """
        Construct a new expression.

        Parameters
        ----------
        name : str
            Name of the operation to apply.
        args : list[Expr]
            List of arguments to the expression.

        Raises
        ------
        TypeError
            If the argument types are invalid for the given function.
        """
        ffi_args = [arg.ffi for arg in args]
        try:
            self.ffi = ffi.Expr(name, ffi_args)
        except TypeError as e:
            e = TypeError(str(e))
            if sys.version_info >= (3, 11):
                # If we can add notes to the exception, indicate the types.
                # This works in Python >=3.11
                for n, arg in enumerate(args):
                    e.add_note(f"Type of arg[{n}]: {arg.data_type()}")
            raise e

    def __repr__(self) -> str:
        """Return an unambiguous string representing the expression."""
        return repr(self.ffi)

    def __str__(self) -> str:
        """Return a readable string representing the expression."""
        return str(self.ffi)

    def data_type(self) -> str:
        """Return a string describing the data type."""
        return self.ffi.data_type_string()

    @final
    def pipe(
        self,
        func: Union[Callable[..., "Expr"], Tuple[Callable[..., "Expr"], str]],
        *args,
        **kwargs,
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

    def __getattr__(self, name: str) -> "Expr":
        """
        Create an expression referencing the given field.

        Parameters
        ----------
        name : str
            Name of the field to access.

        Returns
        -------
        Expr
            Expression referencing the given field.
        """
        return Expr("fieldref", self, Literal(name))

    def __add__(self, rhs: "Expr") -> "Expr":
        """Add two expressions."""
        return Expr("add", self, rhs)

    def __sub__(self, rhs: "Expr") -> "Expr":
        """Subtract two expressions."""
        return Expr("sub", self, rhs)

    def __mul__(self, rhs: "Expr") -> "Expr":
        """Multiple two expressions."""
        return Expr("mul", self, rhs)

    def __truediv__(self, rhs: "Expr") -> "Expr":
        """Divide two expressions."""
        return Expr("div", self, rhs)

    def __lt__(self, rhs: "Expr") -> "Expr":
        """Less than comparison."""
        return Expr("lt", self, rhs)

    def __le__(self, rhs: "Expr") -> "Expr":
        """Less than or equal comparison."""
        return Expr("le", self, rhs)

    def __gt__(self, rhs: "Expr") -> "Expr":
        """Greater than comparison."""
        return Expr("gt", self, rhs)

    def __ge__(self, rhs: "Expr") -> "Expr":
        """Greater than or equal comparison."""
        return Expr("ge", self, rhs)

    def __and__(self, rhs: "Expr") -> "Expr":
        """Logical and."""
        return Expr("and", self, rhs)

    def __or__(self, rhs: "Expr") -> "Expr":
        """Logical or."""
        return Expr("or", self, rhs)

    def __getitem__(self, key: Union["Expr", int, float, str]) -> "Expr":
        """
        Index into an expression.

        If the expression is a struct, the key should be a string corresponding
        to a field.

        If the expression is a list, the key should be an integer index. If the
        expression is a map, the key should be the same type as the map keys.

        Parameters
        ----------
        key : Expr
            The key to index into the expression.

        Returns
        -------
        Expression accessing the given index.
        """
        if isinstance(key, (int, float, str)):
            key = Literal(key)
        return Expr("index", self, key)


class Literal(Expr):
    """A literal expression."""

    def __init__(self, literal: Union[int, float, str]) -> None:
        self.ffi = ffi.Expr("literal", [literal])
