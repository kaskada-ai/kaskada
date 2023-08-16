import kaskada as kd


def test_count_unwindowed(source, golden) -> None:
    m = source.col("m")
    n = source.col("n")
    golden.jsonl(
        kd.record({"m": m, "count_m": m.count(), "n": n, "count_n": n.count()})
    )


def test_count_windowed(source, golden) -> None:
    m = source.col("m")
    n = source.col("n")
    golden.jsonl(
        kd.record(
            {
                "m": m,
                "count_m": m.count(window=kd.windows.Since(m > 20)),
                "n": n,
                "count_n": n.count(window=kd.windows.Sliding(2, m > 10)),
            }
        )
    )


def test_count_since_true(source, golden) -> None:
    # `since(True)` should be the same as unwindowed, so equals to one whenever the value is non-null
    m_sum_since_true = kd.record(
        {
            "m": source.col("m"),
            "m_count": source.col("m").count(window=kd.windows.Since(True)),
        }
    )
    golden.jsonl(m_sum_since_true)
