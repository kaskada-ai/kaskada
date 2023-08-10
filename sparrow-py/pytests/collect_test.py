import pytest
import sparrow_py as kt


@pytest.fixture(scope="module")
def source() -> kt.sources.CsvSource:
    content = "\n".join(
        [
            "time,key,m,n",
            "1996-12-19T16:39:57-08:00,A,5,10",
            "1996-12-19T16:39:58-08:00,B,24,3",
            "1996-12-19T16:39:59-08:00,A,17,6",
            "1996-12-19T16:40:00-08:00,A,,9",
            "1996-12-19T16:40:01-08:00,A,12,",
            "1996-12-19T16:40:02-08:00,A,,",
        ]
    )
    return kt.sources.CsvSource("time", "key", content)


def test_collect_basic(source, golden) -> None:
    m = source.col("m")
    n = source.col("n")
    golden.jsonl(
        kt.record(
            {
                "m": m,
                "collect_m": m.collect(max=None),
                "n": n,
                "collect_n": n.collect(max=None),
            }
        )
    )


def test_collect_with_max(source, golden) -> None:
    m = source.col("m")
    n = source.col("n")
    golden.jsonl(
        kt.record(
            {
                "m": m,
                "collect_m_max_2": m.collect(max=2),
                "n": n,
                "collect_n_max_2": n.collect(max=2),
            }
        )
    )


def test_collect_with_min(source, golden) -> None:
    m = source.col("m")
    n = source.col("n")
    golden.jsonl(
        kt.record(
            {
                "m": m,
                "collect_m_min_2": m.collect(min=2, max=None),
                "n": n,
                "collect_n_min_2": n.collect(min=2, max=None),
            }
        )
    )


def test_collect_with_min_and_max(source, golden) -> None:
    m = source.col("m")
    n = source.col("n")
    golden.jsonl(
        kt.record(
            {
                "m": m,
                "collect_m_min_2_max_2": m.collect(min=2, max=2),
                "n": n,
                "collect_n_min_2_max_2": n.collect(min=2, max=2),
            }
        )
    )


def test_collect_since_window(source, golden) -> None:
    m = source.col("m")
    golden.jsonl(kt.record({"m": m, "since_m": m.sum(window=kt.windows.Since(m > 10))}))



def test_collect_records(source, golden) -> None:
    m = source.col("m")
    n = source.col("n")
    golden.jsonl(kt.record({"m": m, "n": n}).collect(max=None))



def test_collect_records_field_ref(source, golden) -> None:
    m = source.col("m")
    n = source.col("n")
    golden.jsonl(kt.record({"m": m, "n": n}).collect(max=None).col("m"))


def test_collect_lists(source, golden) -> None:
    m = source.col("m")
    golden.jsonl(
        kt.record(
            {
                "m": m,
                "list_m": m.collect(max=10),
                "collect_list": m.collect(max=10).collect(max=10),
            }
        )
    )
