# Benchmarks of Kaskada vs. SQL

Use the `generate.py` to create test data:

```bash
python generate.py --users 500 --items 500 --purchases 10000 --page_views 5000 --reviews 2500
```

This will generate two Parquet files in the current directory.

## Kaskada (Fenl)

Run the statements in the `kaskada.ipynb` using the latest Fenl-supporting Python client.
The RPCs should return the "query time" within each result table.

## DuckDB

Run the queries from `queries_duckdb.sql` one at a time.
With the `enable_profiling` pragma each should report the execution time.

## DataFusion

THe SQL statements aren't ready yet -- they run, but we haven't figured out how to write the results to Parquet to measure end-to-end time.