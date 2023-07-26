"""Defines classes representing Kaskada expressions."""

import sys
from typing import Callable
from typing import Sequence
from typing import Tuple
from typing import Union
from typing import final

import pyarrow as pa
from sparrow_py import _ffi


Arg = Union["Expr", int, str, float]


def _augment_error(args: Sequence[Arg], e: Exception) -> Exception:
    """Augment an error with information about the arguments."""
    if sys.version_info >= (3, 11):
        # If we can add notes to the exception, indicate the types.
        # This works in Python >=3.11
        for n, arg in enumerate(args):
            if isinstance(arg, Expr):
                e.add_note(f"Arg[{n}]: Expr of type {arg.data_type()}")
            else:
                e.add_note(f"Arg[{n}]: Literal {arg} ({type(arg)})")
    return e


class Expr(object):
    """A Kaskada expression."""

    _ffi: _ffi.Expr

    def __init__(self, name: str, *args: Arg) -> None:
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
        ffi_args = [arg._ffi if isinstance(arg, Expr) else arg for arg in args]
        session = next(arg._ffi.session() for arg in args if isinstance(arg, Expr))
        try:
            self._ffi = _ffi.Expr(session=session, operation=name, args=ffi_args)
        except TypeError as e:
            raise _augment_error(args, TypeError(str(e)))
        except ValueError as e:
            raise _augment_error(args, ValueError(str(e)))

    def data_type(self) -> pa.DataType:
        """Return a the data type produced by this expression."""
        return self._ffi.data_type()

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Expr):
            return False
        return self._ffi.equivalent(other._ffi)

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
        # It's easy for this method to cause infinite recursion, if anything
        # it references on `self` isn't defined. To try to avoid this, we only
        # do most of the logic if `self` is a struct type.
        self_type = self.data_type()
        if isinstance(self_type, pa.StructType):
            if self.data_type().get_field_index(name) != -1:
                return Expr("fieldref", self, name)
            else:
                fields = ", ".join(
                    [f"'{self_type[i].name}'" for i in range(self_type.num_fields)]
                )
                raise AttributeError(f"Field '{name}' not found in {fields}")
        else:
            raise TypeError(
                f"Cannot access field '{name}' on non-struct type {self_type.id}"
            )

    def __add__(self, rhs: Arg) -> "Expr":
        """Add two expressions."""
        return Expr("add", self, rhs)

    def __radd__(self, lhs: Arg) -> "Expr":
        """Add two expressions."""
        return Expr("add", lhs, self)

    def __sub__(self, rhs: Arg) -> "Expr":
        """Subtract two expressions."""
        return Expr("sub", self, rhs)

    def __mul__(self, rhs: Arg) -> "Expr":
        """Multiple two expressions."""
        return Expr("mul", self, rhs)

    def __truediv__(self, rhs: Arg) -> "Expr":
        """Divide two expressions."""
        return Expr("div", self, rhs)

    def __lt__(self, rhs: Arg) -> "Expr":
        """Less than comparison."""
        return Expr("lt", self, rhs)

    def __le__(self, rhs: Arg) -> "Expr":
        """Less than or equal comparison."""
        return Expr("le", self, rhs)

    def __gt__(self, rhs: Arg) -> "Expr":
        """Greater than comparison."""
        return Expr("gt", self, rhs)

    def __ge__(self, rhs: Arg) -> "Expr":
        """Greater than or equal comparison."""
        return Expr("ge", self, rhs)

    def __and__(self, rhs: Arg) -> "Expr":
        """Logical and."""
        return Expr("and", self, rhs)

    def __or__(self, rhs: Arg) -> "Expr":
        """Logical or."""
        return Expr("or", self, rhs)

    def __getitem__(self, key: Arg) -> "Expr":
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
        data_type = self.data_type()
        if isinstance(data_type, pa.StructType):
            return Expr("fieldref", self, key)
        elif isinstance(data_type, pa.MapType):
            return Expr("get_map", self, key)
        elif isinstance(data_type, pa.ListType):
            return Expr("get_list", self, key)
        else:
            raise TypeError(f"Cannot index into {data_type}")
