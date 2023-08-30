import kaskada as kd
import kaskada._ffi as _ffi
import pandas as pd
import pytest
from kaskada.udf import Udf


@kd.udf("add<N: number>(x: N, y: N) -> N")
def add(x: pd.Series, y: pd.Series) -> pd.Series:
    """Use Pandas to add two numbers."""
    return x + y


@kd.udf("add_x2<N: number>(x: N, y: N) -> N")
def add_x2(x: pd.Series, y: pd.Series) -> pd.Series:
    """Use Pandas to add then multiply by 2"""
    return (x + y) * 2


def test_udf_instance() -> None:
    assert isinstance(add, Udf)
    assert isinstance(add._ffi, _ffi.Udf)


def test_docstring() -> None:
    assert add.__doc__ == "Use Pandas to add two numbers."


@pytest.fixture
def source_int64() -> kd.sources.CsvString:
    content = "\n".join(
        [
            "time,key,m,n",
            "1996-12-19T16:39:57,A,5,10",
            "1996-12-19T16:39:58,B,24,3",
            "1996-12-19T16:39:59,A,17,6",
            "1996-12-19T16:40:00,A,,9",
            "1996-12-19T16:40:01,A,12,",
            "1996-12-19T16:40:02,A,,",
        ]
    )
    return kd.sources.CsvString(content, time_column="time", key_column="key")


def test_add_udf_direct(golden, source_int64) -> None:
    m = source_int64.col("m")
    n = source_int64.col("n")
    golden.jsonl(kd.record({"m": m, "n": n, "add": add(m, n), "add_x2": add_x2(m, n)}))


def test_add_udf_pipe(golden, source_int64) -> None:
    m = source_int64.col("m")
    n = source_int64.col("n")
    golden.jsonl(
        kd.record(
            {
                "m": m,
                "n": n,
                "add": m.pipe(add, n),
                "add_x2": m.pipe(add_x2, n),
            }
        )
    )
