import kaskada as kd
import pytest


@pytest.fixture(scope="module")
def source() -> kd.sources.CsvString:
    content = "\n".join(
        [
            "time,key,m,new_key",
            "1996-12-19T16:39:57,A,5,C",
            "1996-12-19T16:39:58,B,24,D",
            "1996-12-19T16:39:59,A,17,C",
            "1996-12-19T16:40:00,A,9,C",
            "1996-12-19T16:40:01,A,12,C",
            "1996-12-19T16:40:02,A,,C",
        ]
    )
    return kd.sources.CsvString(content, time_column="time", key_column="key")


def test_with_key_literal(source, golden) -> None:
    golden.jsonl(source.with_key("literal_key"))


def test_with_key_column(source, golden) -> None:
    new_key = source.col("new_key")
    golden.jsonl(source.with_key(new_key))


def test_with_key_grouping(source, golden) -> None:
    new_key = source.col("new_key")
    grouping = "user"
    golden.jsonl(source.with_key(new_key, grouping))


def test_with_key_last(source, golden) -> None:
    golden.jsonl(source.with_key(source.col("new_key")).last())
