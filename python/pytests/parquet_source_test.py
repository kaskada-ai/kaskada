import kaskada as kd


async def test_read_parquet(golden) -> None:
    source = await kd.sources.Parquet.create(
        "../testdata/purchases/purchases_part1.parquet",
        time_column="purchase_time",
        key_column="customer_id",
    )
    golden.jsonl(source)

    await source.add_file("../testdata/purchases/purchases_part2.parquet")
    golden.jsonl(source)
