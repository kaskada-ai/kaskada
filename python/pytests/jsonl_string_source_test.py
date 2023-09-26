import kaskada as kd


async def test_read_jsonl(golden) -> None:
    source = await kd.sources.JsonlString.create(
        '{"time": "1996-12-19T16:39:57", "user": "A", "m": 5, "n": 10}',
        time_column="time",
        key_column="user",
    )
    golden.jsonl(source)

    await source.add_string(
        """
        {"time": "1996-12-19T16:40:57", "user": "A", "m": 8, "n": 10}
        {"time": "1996-12-19T16:41:57", "user": "B", "m": 5}
        """
    )

    golden.jsonl(source)


async def test_read_jsonl_lists(golden) -> None:
    source = await kd.sources.JsonlString.create(
        '{"time": "1996-12-19T16:39:57", "user": "A", "m": [5, 10], "n": 10}',
        time_column="time",
        key_column="user",
    )
    golden.jsonl(source)

    await source.add_string(
        """
        {"time": "1996-12-19T16:40:57", "user": "A", "m": [], "n": 10}
        {"time": "1996-12-19T16:41:57", "user": "A", "n": 10}
        """
    )
    golden.jsonl(source)


async def test_read_jsonl_with_subsort(golden) -> None:
    source = await kd.sources.JsonlString.create(
        '{"time": "1996-12-19T16:39:57", "user": "A", "m": 5, "n": 10, "subsort": 1}',
        time_column="time",
        key_column="user",
        subsort_column="subsort",
    )
    golden.jsonl(source)

    await source.add_string(
        """
        {"time": "1996-12-19T16:40:57", "user": "A", "m": 8, "n": 10, "subsort": 2}
        {"time": "1996-12-19T16:41:57", "user": "B", "m": 5, "subsort": 3}
        """
    )

    golden.jsonl(source)
