import kaskada as kd


def test_min_unwindowed(source, golden) -> None:
    m = source.col("m")
    n = source.col("n")
    golden.jsonl(kd.record({"m": m, "min_m": m.min(), "n": n, "min_n": n.min()}))


def test_min_windowed(source, golden) -> None:
    m = source.col("m")
    n = source.col("n")
    golden.jsonl(
        kd.record(
            {
                "m": m,
                "min_m": m.min(window=kd.windows.Since(m > 20)),
                "n": n,
                "min_n": n.min(window=kd.windows.Sliding(2, m > 10)),
            }
        )
    )


def test_min_since_true(source, golden) -> None:
    # `since(True)` should be the same as unwindowed, so equals the original vaule.
    m_min_since_true = kd.record(
        {
            "m": source.col("m"),
            "m_min": source.col("m").min(window=kd.windows.Since(True)),
        }
    )
    golden.jsonl(m_min_since_true)
