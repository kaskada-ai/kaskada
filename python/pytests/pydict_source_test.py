import kaskada as kd
import pyarrow as pa
import pytest


def test_read_pydict(golden) -> None:
    source = kd.sources.PyDict(
        [{"time": "1996-12-19T16:39:57", "user": "A", "m": 5, "n": 10}],
        time_column="time",
        key_column="user",
    )
    golden.jsonl(source)

    source.add_rows(
        [
            {"time": "1996-12-19T16:40:57", "user": "A", "m": 8, "n": 10},
            {"time": "1996-12-19T16:41:57", "user": "B", "m": 5},
        ]
    )

    golden.jsonl(source)
    source.add_rows({"time": "1996-12-19T16:42:57", "user": "A", "m": 8, "n": 10})
    golden.jsonl(source)


def test_read_pydict_lists(golden) -> None:
    source = kd.sources.PyDict(
        [{"time": "1996-12-19T16:39:57", "user": "A", "m": [5, 10], "n": 10}],
        time_column="time",
        key_column="user",
    )
    golden.jsonl(source)

    source.add_rows(
        [
            {"time": "1996-12-19T16:40:57", "user": "A", "m": [], "n": 10},
            {"time": "1996-12-19T16:41:57", "user": "A", "n": 10},
        ]
    )
    golden.jsonl(source)


def test_read_pydict_ignore_column(golden) -> None:
    # Schema is determined from first row, and doesn't contain an "m" column.
    source = kd.sources.PyDict(
        [{"time": "1996-12-19T16:39:57", "user": "A", "n": 10}],
        time_column="time",
        key_column="user",
    )
    golden.jsonl(source)

    source.add_rows(
        [
            {"time": "1996-12-19T16:40:57", "user": "A", "m": 83, "n": 10},
            {"time": "1996-12-19T16:41:57", "user": "A", "m": 12},
        ]
    )
    golden.jsonl(source)


def test_read_pydict_empty() -> None:
    source = kd.sources.PyDict(
        rows=[],
        schema=pa.schema(
            [
                ("time", pa.timestamp("ns")),
                ("user", pa.string()),
                ("x", pa.int64()),
            ]
        ),
        time_column="time",
        key_column="user",
    )

    result = source.preview()

    assert result.empty
    assert list(result.columns) == ["_time", "_key", "time", "user", "x"]
    assert result["x"].dtype == "int64"


@pytest.mark.asyncio
async def test_read_pydict_empty_live(golden) -> None:
    source = kd.sources.PyDict(
        rows=[],
        schema=pa.schema(
            [
                ("time", pa.string()),
                ("user", pa.string()),
                ("x", pa.int64()),
            ]
        ),
        time_column="time",
        key_column="user",
    )

    execution = source.run_iter(mode="live")
    source.add_rows({"time": "1996-12-19T16:39:57Z", "user": "A", "x": 5})
    golden.jsonl(await execution.__anext__())

    execution.stop()
    with pytest.raises(StopAsyncIteration):
        print(await execution.__anext__())
