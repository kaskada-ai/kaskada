import collections
from typing import NamedTuple

import kaskada as kd
import pyarrow as pa
import pytest


SourceCols = NamedTuple(
    "SourceCols",
    [("m", kd.Timestream), ("n", kd.Timestream), ("datatype", pa.DataType)],
)

Data = collections.namedtuple("Data", "m, n, param")


def datatype_id(datatype: pa.DataType) -> str:
    return str(datatype)


@pytest.fixture(
    scope="module",
    params=[pa.int8(), pa.uint8(), pa.int64(), pa.uint64(), pa.float64()],
    ids=datatype_id,
)
async def source(request) -> SourceCols:
    datatype = request.param
    source = await kd.sources.PyDict.create(
        schema=pa.schema(
            [
                ("time", pa.string()),
                ("user", pa.string()),
                ("m", datatype),
                ("n", datatype),
            ]
        ),
        rows=[
            {"time": "1996-12-19T16:39:57", "user": "A", "m": 5, "n": 10},
            {"time": "1996-12-19T16:39:58", "user": "B", "m": 24, "n": 3},
            {"time": "1996-12-19T16:39:59", "user": "A", "m": 17, "n": 6},
            {"time": "1996-12-19T16:40:00", "user": "A", "m": None, "n": 9},
            {"time": "1996-12-19T16:40:01", "user": "A", "m": 12, "n": 12},
            {"time": "1996-12-19T16:40:02", "user": "A", "m": None, "n": None},
            {"time": "1996-12-19T16:40:03", "user": "A", "m": 12.3, "n": 6.4},
            {"time": "1996-12-19T16:40:04", "user": "A", "m": 12.7, "n": 8.7},
        ],
        time_column="time",
        key_column="user",
    )

    return SourceCols(m=source.col("m"), n=source.col("n"), datatype=datatype)


async def test_add(source, golden) -> None:
    golden.jsonl(
        kd.record({"m": source.m, "n": source.n, "result": source.m + source.n})
    )


async def test_add_literal(source, golden) -> None:
    golden.jsonl(kd.record({"m": source.m, "result": source.m + 1}))


async def test_sub(source, golden) -> None:
    if pa.types.is_unsigned_integer(source.datatype):
        pytest.skip("unsigned integers currently underflows")

    golden.jsonl(
        kd.record({"m": source.m, "n": source.n, "result": source.m - source.n})
    )


async def test_sub_literal(source, golden) -> None:
    golden.jsonl(kd.record({"m": source.m, "result": source.m - 1}))


async def test_mul(source, golden) -> None:
    if source.datatype == pa.int8():
        pytest.skip("int8 currently overflows on multiplication")
    golden.jsonl(
        kd.record({"m": source.m, "n": source.n, "result": source.m * source.n})
    )


async def test_mul_literal(source, golden) -> None:
    golden.jsonl(kd.record({"m": source.m, "result": source.m * 2}))


async def test_div(source, golden) -> None:
    golden.jsonl(
        kd.record({"m": source.m, "n": source.n, "result": source.m / source.n})
    )


async def test_div_literal(source, golden) -> None:
    golden.jsonl(kd.record({"m": source.m, "result": source.m / 2}))


async def test_neg(source, golden) -> None:
    golden.jsonl(kd.record({"m": source.m, "result": source.m.neg()}))


async def test_ceil(source, golden) -> None:
    golden.jsonl(kd.record({"m": source.m, "result": source.m.ceil()}))


async def test_floor(source, golden) -> None:
    golden.jsonl(source.m.floor())


async def test_round(source, golden) -> None:
    golden.jsonl(kd.record({"m": source.m, "result": source.m.round()}))


async def test_greatest(source, golden) -> None:
    golden.jsonl(
        kd.record({"m": source.m, "n": source.n, "result": source.m.greatest(source.n)})
    )


async def test_greatest_literal(source, golden) -> None:
    golden.jsonl(kd.record({"m": source.m, "result": source.m.greatest(14)}))


async def test_least(source, golden) -> None:
    golden.jsonl(
        kd.record({"m": source.m, "n": source.n, "result": source.m.least(source.n)})
    )


async def test_least_literal(source, golden) -> None:
    golden.jsonl(kd.record({"m": source.m, "result": source.m.least(4)}))


async def test_powf(source, golden) -> None:
    golden.jsonl(
        kd.record({"m": source.m, "n": source.n, "result": source.m.powf(source.n)})
    )


async def test_powf_literal(source, golden) -> None:
    golden.jsonl(kd.record({"m": source.m, "result": source.m.powf(2)}))


async def test_exp(source, golden) -> None:
    golden.jsonl(kd.record({"m": source.m, "result": source.m.exp()}))


async def test_clamp_min(source, golden) -> None:
    golden.jsonl(
        kd.record(
            {"m": source.m, "n": source.n, "result": source.m.clamp(min=source.n)}
        )
    )


async def test_clamp_max(source, golden) -> None:
    golden.jsonl(
        kd.record(
            {"m": source.m, "n": source.n, "result": source.m.clamp(max=source.n)}
        )
    )


async def test_clamp_min_literal_max(source, golden) -> None:
    golden.jsonl(
        kd.record(
            {
                "m": source.m,
                "n": source.n,
                "result": source.m.clamp(min=source.n, max=8),
            }
        )
    )


async def test_clamp_max_literal_min(source, golden) -> None:
    golden.jsonl(
        kd.record(
            {
                "m": source.m,
                "n": source.n,
                "result": source.m.clamp(min=0, max=source.n),
            }
        )
    )


async def test_clamp_literal(source, golden) -> None:
    golden.jsonl(kd.record({"m": source.m, "result": source.m.clamp(min=0, max=10)}))


async def test_sqrt(source, golden) -> None:
    golden.jsonl(kd.record({"m": source.m, "result": source.m.sqrt()}))
