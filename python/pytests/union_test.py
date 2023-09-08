import kaskada as kd


def test_union(golden) -> None:
    source = kd.sources.PyDict(
        [
            {"time": "1996-12-19T16:39:57", "user": "A", "m": [5], "n": []},
            {"time": "1996-12-19T17:39:57", "user": "A", "m": [], "n": [5, 6]},
            {"time": "1996-12-19T18:39:57", "user": "A", "m": [None]},
            {"time": "1996-12-19T19:39:57", "user": "A", "m": [6, 7], "n": [6, 7, 8]},
            {
                "time": "1996-12-19T19:39:57",
                "user": "A",
                "m": [6, 7, 8, 6],
                "n": [9, 8, 10],
            },
        ],
        time_column="time",
        key_column="user",
    )

    golden.jsonl(source.col("m").union(source.col("n")))
