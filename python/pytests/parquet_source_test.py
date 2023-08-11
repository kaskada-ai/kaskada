import kaskada as kd


def test_read_parquet(golden) -> None:
    source = kd.sources.Parquet(
        "../testdata/purchases/purchases_part1.parquet",
        time_column_name="purchase_time",
        key_column_name="customer_id",
    )
    golden.jsonl(source)

    source.add_file("../testdata/purchases/purchases_part2.parquet")
    golden.jsonl(source)
