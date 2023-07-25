from sparrow_py.udf import Udf, fenl_udf
from sparrow_py.ffi import call_udf
import pandas as pd
import pyarrow as pa

@fenl_udf("add", "add(x: number, y: number) -> number")
def add(x: pd.Series, y: pd.Series) -> pd.Series:
    return x + y

def test_numeric_udf_pure_python() -> None:
    """Test the python side of UDFs."""
    assert type(add) == Udf

    x = pa.array([1, 12, 17, 23, 28], type=pa.int8())
    y = pa.array([1, 13, 18, 20, 4], type=pa.int8())
    result = add.run_pyarrow(pa.int8(), x, y)
    print(result)
    assert result == pa.array([2, 25, 35, 43, 32], type=pa.int8())


def test_numeric_udf_rust() -> None:
    """Test the rust side of UDFs."""
    x = pa.array([1, 12, 17, 23, 28], type=pa.int8())
    y = pa.array([1, 13, 18, 20, 4], type=pa.int8())
    result = call_udf(add, pa.int8(), x, y)
    print(result)
    assert result == pa.array([2, 25, 35, 43, 32], type=pa.int8())