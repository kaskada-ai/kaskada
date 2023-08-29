import kaskada as kd
import pytest


@pytest.fixture
def source() -> kd.sources.CsvString:
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


def test_record(source, golden) -> None:
    m = source.col("m")
    n = source.col("n")

    golden.jsonl(
        kd.record(
            {
                "m": m,
                "n": n,
            }
        )
    )


def test_extend_dict(source, golden) -> None:
    m = source.col("m")
    n = source.col("n")
    golden.jsonl(source.extend({"add": m + n}))


def test_extend_record(source, golden) -> None:
    m = source.col("m")
    n = source.col("n")
    golden.jsonl(source.extend(kd.record({"add": m + n})))


def test_extend_computed_record(source, golden) -> None:
    golden.jsonl(source.extend(lambda i: kd.record({"add": i.col("m") + i.col("n")})))


def test_extend_computed_dict(source, golden) -> None:
    golden.jsonl(source.extend(lambda input: {"add": input.col("m") + input.col("n")}))


def test_select_record(source, golden) -> None:
    golden.jsonl(source.select("n"))


def test_remove_record(source, golden) -> None:
    golden.jsonl(source.remove("n"))
