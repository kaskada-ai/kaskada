import kaskada as kd


def test_flatten(golden) -> None:
    source = kd.sources.PyList(
        [
            {"time": "1996-12-19T16:39:57", "user": "A", "m": [[5]]},
            {"time": "1996-12-19T17:39:57", "user": "A", "m": []},
            {"time": "1996-12-19T18:39:57", "user": "A", "m": [None]},
            {"time": "1996-12-19T19:39:57", "user": "A", "m": [[6], [7]]},
            {"time": "1996-12-19T19:39:57", "user": "A", "m": [[7, 8], [9, 10]]},
        ],
        time_column_name="time",
        key_column_name="user",
    )

    golden.jsonl(source.col("m").flatten())
