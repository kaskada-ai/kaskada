"""Functionality for calling Python UDFs from Kaskada."""
from __future__ import annotations

import functools
from typing import Callable

import pandas as pd
import pyarrow as pa

from . import _ffi
from ._timestream import Literal
from ._timestream import Timestream


# TODO: Allow functions to return `pd.DataFrame` for struct arrays.
FuncType = Callable[..., pd.Series]


class Udf:
    """User defined function."""

    def __init__(self, signature: str, func: Callable[..., pa.Array]) -> None:
        """Create a UDF with the given signature."""
        functools.update_wrapper(self, func)
        self._ffi = _ffi.Udf(signature, func)

    def __call__(self, *args: Timestream | Literal) -> Timestream:
        """Apply the UDF to the given arguments."""
        return Timestream._call(self._ffi, *args)


def udf(signature: str):
    """Decorate a function for use as a Kaskada UDF."""

    def decorator(func_type: FuncType):
        # 1. Convert the `FuncType` to the type expected by Udf.
        # This needs to take the PyArrow result type and PyArrow inputs,
        # convert them to Pandas, call the function, and convert the result
        # back to PyArrow.
        def func(result_type, *args):
            return _converted_func(func_type, result_type, *args)

        # 2. Create the UDF object.
        return Udf(signature, func)

    return decorator


def _converted_func(
    func_type: FuncType, result_type: pa.DataType, *args: pa.Array
) -> pa.Array:
    """Run the function producing the given result type."""
    # TODO: I believe this will return a series for simple arrays, and a
    # dataframe for struct arrays. We should explore how this handles
    # different types.
    pd_args = [arg.to_pandas() for arg in args]
    pd_result = func_type(*pd_args)

    print("Result type: ", result_type)
    if isinstance(pd_result, pd.Series):
        return pa.Array.from_pandas(pd_result, type=result_type)
    else:
        raise TypeError(f"Unsupported result type: {type(pd_result)}")
