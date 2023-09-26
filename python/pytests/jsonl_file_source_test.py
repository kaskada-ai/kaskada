import kaskada as kd


async def test_read_jsonl_file(golden) -> None:
    source = await kd.sources.JsonlFile.create(
        "../testdata/purchases/purchases_part1.jsonl",
        time_column="purchase_time",
        key_column="customer_id",
    )
    golden.jsonl(source)

    await source.add_file("../testdata/purchases/purchases_part2.jsonl")
    golden.jsonl(source)


async def test_read_jsonl_file_with_subsort(golden) -> None:
    source = await kd.sources.JsonlFile.create(
        "../testdata/purchases/purchases_part1.jsonl",
        time_column="purchase_time",
        key_column="customer_id",
        subsort_column="subsort_id",
    )
    golden.jsonl(source)

    await source.add_file("../testdata/purchases/purchases_part2.jsonl")
    golden.jsonl(source)
