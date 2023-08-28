import kaskada as kd
import pandas as pd


def test_float_seconds(golden) -> None:
    data = {"time": [1671477472.026119], "user": ["tom"]}
    df = pd.DataFrame(data)
    table = kd.sources.Pandas(df, time_column="time", key_column="user", time_unit="s")

    golden.jsonl(table)


def test_float_milliseconds(golden) -> None:
    data = {"time": [1671477472026.119], "user": ["tom"]}
    df = pd.DataFrame(data)
    table = kd.sources.Pandas(df, time_column="time", key_column="user", time_unit="ms")

    golden.jsonl(table)


def test_float_nanoseconds(golden) -> None:
    data = {"time": [1671477472026119000], "user": ["tom"]}
    df = pd.DataFrame(data)
    table = kd.sources.Pandas(df, time_column="time", key_column="user")

    golden.jsonl(table)


def test_add_dataframe(golden) -> None:
    df1 = pd.DataFrame(
        {
            "time": [1000000000000],
            "key": ["a"],
        }
    )

    table = kd.sources.Pandas(df1, time_column="time", key_column="key")
    golden.jsonl(table)

    df2 = pd.DataFrame(
        {
            "time": [1000000000000],
            "key": ["a"],
        }
    )
    table.add_data(df2)
    golden.jsonl(table)
