import pytest
import sparrow_py as kt


@pytest.fixture(scope="module")
def source() -> kt.sources.CsvSource:
    content = "\n".join(
        [
            "time,key,m,new_key",
            "1996-12-19T16:39:57-08:00,A,5,C",
            "1996-12-19T16:39:58-08:00,B,24,D",
            "1996-12-19T16:39:59-08:00,A,17,C",
            "1996-12-19T16:40:00-08:00,A,9,C",
            "1996-12-19T16:40:01-08:00,A,12,C",
            "1996-12-19T16:40:02-08:00,A,,C",
        ]
    )
    return kt.sources.CsvSource("time", "key", content)

def test_with_key_literal(source, golden) -> None:
    golden.jsonl(source.with_key("literal_key"))


def test_with_key_column(source, golden) -> None:
    new_key = source.col("new_key")
    golden.jsonl(source.with_key(new_key))

def test_with_key_grouping(source, golden) -> None:
    new_key = source.col("new_key")
    grouping = "user"
    golden.jsonl(source.with_key(new_key, grouping))

