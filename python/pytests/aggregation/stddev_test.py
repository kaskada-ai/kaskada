import kaskada as kd


def test_stddev_unwindowed(source, golden) -> None:
    m = source.col("m")
    n = source.col("n")
    golden.jsonl(
        kd.record({"m": m, "stddev_m": m.stddev(), "n": n, "stddev_n": n.stddev()})
    )


def test_stddev_windowed(source, golden) -> None:
    m = source.col("m")
    n = source.col("n")
    golden.jsonl(
        kd.record(
            {
                "m": m,
                "stddev_m": m.stddev(window=kd.windows.Since(m > 20)),
                "n": n,
                "stddev_n": n.stddev(window=kd.windows.Sliding(2, m > 10)),
            }
        )
    )


def test_stddev_since_true(source, golden) -> None:
    # `since(True)` should be the same as unwindowed, so equals the original vaule.
    m_stddev_since_true = kd.record(
        {
            "m": source.col("m"),
            "m_stddev": source.col("m").stddev(window=kd.windows.Since(True)),
        }
    )
    golden.jsonl(m_stddev_since_true)
