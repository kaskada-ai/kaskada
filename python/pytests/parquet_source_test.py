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


# Verifies that we drain the output and progress channels correctly.
#
# When the parquet file contains more rows than
# (CHANNEL_SIZE / MAX_BATCH_SIZE), the channels previously filled
# up, causing the sender to block. This test verifies that the
# channels correctly drain, allowing the sender to continue.
# See https://github.com/kaskada-ai/kaskada/issues/775
async def test_large_parquet_file(golden) -> None:
    source = await kd.sources.Parquet.create(
        "../testdata/parquet/purchases_100k.parquet",
        time_column="time",
        key_column="user",
    )
    user = source.col("user")
    amount = source.col("amount")

    # Add a filter to reduce the output file size while ensuring the entire
    # file is still processed
    predicate = user.eq("5fec83d4-f5c6-4943-ab05-2b6760330daf").and_(amount.gt(490))
    golden.jsonl(source.filter(predicate))
