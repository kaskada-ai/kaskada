import kaskada as kd


def test_sum_unwindowed(source, golden) -> None:
    m = source.col("m")
    n = source.col("n")
    golden.jsonl(kd.record({"m": m, "sum_m": m.sum(), "n": n, "sum_n": n.sum()}))


def test_sum_windowed(source, golden) -> None:
    m = source.col("m")
    n = source.col("n")
    golden.jsonl(
        kd.record(
            {
                "m": m,
                "sum_m": m.sum(window=kd.windows.Since(m > 20)),
                "n": n,
                "sum_n": n.sum(window=kd.windows.Sliding(2, m > 10)),
            }
        )
    )


def test_sum_since_true(source, golden) -> None:
    # `since(True)` should be the same as unwindowed, so equals the original vaule.
    m_sum_since_true = kd.record(
        {
            "m": source.col("m"),
            "m_sum": source.col("m").sum(window=kd.windows.Since(True)),
        }
    )
    golden.jsonl(m_sum_since_true)
