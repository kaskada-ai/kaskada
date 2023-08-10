import pandas as pd
import pyarrow as pa
from sparrow_py.udf import Udf
from sparrow_py.udf import fenl_udf

from sparrow_py._ffi import call_udf


@fenl_udf("add", "add(x: number, y: number) -> number")
def add(x: pd.Series, y: pd.Series) -> pd.Series:
    return x + y


def test_numeric_udf_pure_python() -> None:
    assert isinstance(add, Udf)

    x = pa.array([1, 12, 17, 23, 28], type=pa.int8())
    y = pa.array([1, 13, 18, 20, 4], type=pa.int8())
    result = add.run_pyarrow(pa.int8(), x, y)
    assert result == pa.array([2, 25, 35, 43, 32], type=pa.int8())


def test_numeric_udf_rust() -> None:
    x = pa.array([1, 12, 17, 23, 28], type=pa.int8())
    y = pa.array([1, 13, 18, 20, 4], type=pa.int8())
    result = call_udf(add, pa.int8(), x, y)
    assert result == pa.array([2, 25, 35, 43, 32], type=pa.int8())
