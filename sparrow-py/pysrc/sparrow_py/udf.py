import functools
import pandas as pd
import pyarrow as pa
from typing import Callable

# TODO: Allow functions to return `pd.DataFrame` for struct arrays.
FuncType = Callable[..., pd.Series]

class Udf(object):
    def __init__(self, name, func: FuncType, signature: str) -> None:
        functools.update_wrapper(self, func)
        self.name = name
        self.func = func
        self.signature = signature

    def run_pyarrow(self, result_type: pa.DataType, *args: pa.Array) -> pa.Array:
        # TODO: I believe this will return a series for simple arrays, and a
        # dataframe for struct arrays. We should explore how this handles
        # different types.
        pd_args = [arg.to_pandas() for arg in args]
        pd_result = self.func(*pd_args)

        if isinstance(pd_result, pd.Series):
            return pa.Array.from_pandas(pd_result, type=result_type)
        else:
            raise TypeError(f'Unsupported result type: {type(pd_result)}')


def fenl_udf(name: str, signature: str):
    def decorator(func: FuncType):
        print(type(func))
        return Udf(name, func, signature)
    return decorator