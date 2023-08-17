import kaskada as kd


def test_variance_unwindowed(source, golden) -> None:
    m = source.col("m")
    n = source.col("n")
    golden.jsonl(
        kd.record(
            {"m": m, "variance_m": m.variance(), "n": n, "variance_n": n.variance()}
        )
    )


def test_variance_windowed(source, golden) -> None:
    m = source.col("m")
    n = source.col("n")
    golden.jsonl(
        kd.record(
            {
                "m": m,
                "variance_m": m.variance(window=kd.windows.Since(m > 20)),
                "n": n,
                "variance_n": n.variance(window=kd.windows.Sliding(2, m > 10)),
            }
        )
    )


def test_variance_since_true(source, golden) -> None:
    # `since(True)` should be the same as unwindowed, so equals the original vaule.
    m_variance_since_true = kd.record(
        {
            "m": source.col("m"),
            "m_variance": source.col("m").variance(window=kd.windows.Since(True)),
        }
    )
    golden.jsonl(m_variance_since_true)
