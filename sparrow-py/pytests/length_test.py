import pytest
import sparrow_py as kt


@pytest.fixture(scope="module")
def source() -> kt.sources.CsvSource:
    content = "\n".join(
        [
            "time,key,m,n,str",
            "1996-12-19T16:39:57-08:00,A,5,10,\"apple\"",
            "1996-12-19T16:39:58-08:00,B,24,3,\"dog\"",
            "1996-12-19T16:39:59-08:00,A,17,6,\"carrot\"",
            "1996-12-19T16:40:00-08:00,A,,9,",
            "1996-12-19T16:40:01-08:00,A,12,,\"eggplant\"",
            "1996-12-19T16:40:02-08:00,A,,,\"fig\"",
        ]
    )
    return kt.sources.CsvSource("time", "key", content)


def test_length(source, golden) -> None:
    my_str = source["str"]
    list = my_str.collect(max=None)
    golden.jsonl(
        kt.record(
            {
                "str": my_str,
                "len_key": my_str.length(),
                "list": list,
                "len_list": list.length(),
            }
        )
    )
