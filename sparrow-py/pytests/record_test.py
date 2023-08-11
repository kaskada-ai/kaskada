import pytest

import sparrow_py as kt


@pytest.fixture
def source() -> kt.sources.CsvString:
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
    return kt.sources.CsvString(content, time_column_name="time", key_column_name="key")


def test_record(source, golden) -> None:
    m = source.col("m")
    n = source.col("n")

    golden.jsonl(
        kt.record(
            {
                "m": m,
                "n": n,
            }
        )
    )


def test_extend_record(source, golden) -> None:
    m = source.col("m")
    n = source.col("n")
    golden.jsonl(source.extend({"add": m + n}))


def test_extend_input(source, golden) -> None:
    golden.jsonl(source.extend(lambda input: {"add": input.col("m") + input.col("n")}))


def test_select_record(source, golden) -> None:
    golden.jsonl(source.select("n"))


def test_remove_record(source, golden) -> None:
    golden.jsonl(source.remove("n"))
