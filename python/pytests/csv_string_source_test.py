import kaskada as kd


def test_read_csv(golden) -> None:
    content1 = "\n".join(
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
    content2 = "\n".join(
        [
            "time,key,m,n",
            "1996-12-19T17:39:57,A,5,10",
            "1996-12-19T17:39:58,B,24,3",
            "1996-12-19T17:39:59,A,17,6",
            "1996-12-19T17:40:00,A,,9",
            "1996-12-19T17:40:01,A,12,",
            "1996-12-19T17:40:02,A,,",
        ]
    )
    source = kd.sources.CsvString(
        content1,
        time_column="time",
        key_column="key",
    )
    golden.jsonl(source)

    source.add_string(content2)
    golden.jsonl(source)
