"""Functionality for calling Python UDFs from Kaskada."""
import functools
from typing import Callable

import pandas as pd
import pyarrow as pa


# TODO: Allow functions to return `pd.DataFrame` for struct arrays.
FuncType = Callable[..., pd.Series]


class Udf(object):
    """Class wrapping a UDF used in Kaskada."""

    def __init__(self, name, func: FuncType, signature: str) -> None:
        """Create a UDF for a function returning a Pandas series.

        Parameters
        ----------
        name : str
            Name of the function being wrapped.

        func : FuncType
            The callable to wrap.

        signature : str
            The Kaskada function signature for this UDF. Will be used to check
            types of calls to this function and propagate type information for
            the rest of the query.
        """
        functools.update_wrapper(self, func)
        self.name = name
        self.func = func
        self.signature = signature

    def run_pyarrow(self, result_type: pa.DataType, *args: pa.Array) -> pa.Array:
        """Run the function producing the given result type."""
        # TODO: I believe this will return a series for simple arrays, and a
        # dataframe for struct arrays. We should explore how this handles
        # different types.
        pd_args = [arg.to_pandas() for arg in args]
        pd_result = self.func(*pd_args)

        if isinstance(pd_result, pd.Series):
            return pa.Array.from_pandas(pd_result, type=result_type)
        else:
            raise TypeError(f"Unsupported result type: {type(pd_result)}")


def fenl_udf(name: str, signature: str):
    """Decorate a function for use as a Kaskada UDF."""

    def decorator(func: FuncType):
        return Udf(name, func, signature)

    return decorator
