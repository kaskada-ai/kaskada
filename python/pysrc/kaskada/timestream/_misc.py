from __future__ import annotations

from typing import (
    Callable,
    Tuple,
    Union,
    final,
)

import pyarrow as pa

from . import Timestream, Arg

def cast(self, data_type: pa.DataType) -> Timestream:
    """Return this Timestream cast to the given data type.

    Args:
        data_type: The DataType to cast to.
    """
    return Timestream(self._ffi_expr.cast(data_type))

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


def else_(self, other: Timestream) -> Timestream:
    """Return a Timestream containing `self` when not `null`, and `other` otherwise.

    Args:
        other: The Timestream to use if self is `null`.
    """
    return Timestream._call("else", other, self)

def filter(self, condition: Arg) -> Timestream:
    """Return a Timestream containing only the points where `condition` is `true`.

    Args:
        condition: The condition to filter on.
    """
    return Timestream._call("when", condition, self)

def hash(self) -> Timestream:
    """Return a Timestream containing the hash of the input.

    Notes:
        This will only return `null` when interpolated at points where
        the input is not defined. At other points, it will return the
        hash of `null`, which is `0` (not `null`).
    """
    return Timestream._call("hash", self)

def if_(self, condition: Arg) -> Timestream:
    """Return a `Timestream` from `self` at points where `condition` is `true`, and `null` otherwise.

    Args:
        condition: The condition to check.
    """
    return Timestream._call("if", condition, self, input=self)

def lag(self, n: int) -> Timestream:
    """Return a Timestream containing the value `n` points before each point.

    Args:
        n: The number of points to lag by.
    """
    # hack to support structs/lists (as collect supports lists)
    return self.collect(max=n + 1, min=n + 1)[0]

def null_if(self, condition: Arg) -> Timestream:
    """Return a `Timestream` from `self` at points where `condition` is not `false`, and `null` otherwise.

    Args:
        condition: The condition to check.
    """
    return Timestream._call("null_if", condition, self, input=self)

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
