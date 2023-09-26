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


async def test_read_parquet_with_subsort(golden) -> None:
    source = await kd.sources.Parquet.create(
        "../testdata/purchases/purchases_part1.parquet",
        time_column="purchase_time",
        key_column="customer_id",
        subsort_column="subsort_id",
    )
    golden.jsonl(source)

    await source.add_file("../testdata/purchases/purchases_part2.parquet")
    golden.jsonl(source)


async def test_time_column_as_float_can_cast_s(golden) -> None:
    source = await kd.sources.Parquet.create(
        "../testdata/purchases/purchases_float_time.parquet",
        time_column="purchase_time",
        key_column="customer_id",
        time_unit="s",
    )
    golden.jsonl(source)

async def test_time_column_as_float_can_cast_ns(golden) -> None:
    source = await kd.sources.Parquet.create(
        "../testdata/purchases/purchases_float_time.parquet",
        time_column="purchase_time",
        key_column="customer_id",
    )
    golden.jsonl(source)

