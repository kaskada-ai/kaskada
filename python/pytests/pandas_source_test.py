import kaskada as kd
import pandas as pd


async def test_float_seconds(golden) -> None:
    data = {"time": [1671477472.026119], "user": ["tom"]}
    df = pd.DataFrame(data)
    table = await kd.sources.Pandas.create(
        df, time_column="time", key_column="user", time_unit="s"
    )

    golden.jsonl(table)


async def test_float_milliseconds(golden) -> None:
    data = {"time": [1671477472026.119], "user": ["tom"]}
    df = pd.DataFrame(data)
    table = await kd.sources.Pandas.create(
        df, time_column="time", key_column="user", time_unit="ms"
    )

    golden.jsonl(table)


async def test_float_nanoseconds(golden) -> None:
    data = {"time": [1671477472026119000], "user": ["tom"]}
    df = pd.DataFrame(data)
    table = await kd.sources.Pandas.create(
        df, time_column="time", key_column="user", time_unit="ns"
    )

    golden.jsonl(table)


async def test_add_dataframe(golden) -> None:
    df1 = pd.DataFrame(
        {
            "time": [1000000000000],
            "key": ["a"],
        }
    )

    table = await kd.sources.Pandas.create(df1, time_column="time", key_column="key")
    golden.jsonl(table)

    df2 = pd.DataFrame(
        {
            "time": [1000000000000],
            "key": ["a"],
        }
    )
    await table.add_data(df2)
    golden.jsonl(table)


async def test_add_dataframe_with_subsort(golden) -> None:
    df1 = pd.DataFrame(
        {
            "time": [1000000000000],
            "key": ["a"],
            "subsort": [1],
        }
    )

    table = await kd.sources.Pandas.create(
        df1,
        time_column="time",
        key_column="key",
        subsort_column="subsort",
    )
    golden.jsonl(table)

    df2 = pd.DataFrame(
        {
            "time": [1000000000000],
            "key": ["a"],
            "subsort": [2],
        }
    )
    await table.add_data(df2)
    golden.jsonl(table)
