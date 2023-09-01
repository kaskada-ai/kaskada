from datetime import timedelta

import kaskada as kd
import pytest


@pytest.fixture(scope="module")
def source() -> kd.sources.CsvString:
    content = "\n".join(
        [
            "time,key,m,n,s,b",
            "1996-12-19T16:39:57,A,5,10,a,true",
            "1996-12-19T16:39:58,B,24,3,b,true",
            "1996-12-19T16:39:59,A,17,6,b,",
            "1996-12-19T16:40:00,A,,9,,false",
            "1996-12-19T16:40:01,A,12,,e,false",
            "1996-12-19T16:40:02,A,,,f,true",
            "1996-12-19T16:40:04,A,,,f,",
        ]
    )
    return kd.sources.CsvString(content, time_column="time", key_column="key")


def test_collect_basic(source, golden) -> None:
    m = source.col("m")
    n = source.col("n")
    golden.jsonl(
        kd.record(
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
        kd.record(
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
        kd.record(
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
        kd.record(
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
    golden.jsonl(
        kd.record(
            {"m": m, "since_m": m.collect(max=None, window=kd.windows.Since(m > 10))}
        )
    )


def test_collect_i64_trailing_window_1s(source, golden) -> None:
    m = source.col("m")
    golden.jsonl(
        kd.record(
            {
                "m": m,
                "collect_m": m.collect(
                    max=None, window=kd.windows.Trailing(timedelta(seconds=1))
                ),
            }
        )
    )


def test_collect_i64_trailing_window_3s(source, golden) -> None:
    m = source.col("m")
    golden.jsonl(
        kd.record(
            {
                "m": m,
                "collect_m": m.collect(
                    max=None, window=kd.windows.Trailing(timedelta(seconds=3))
                ),
            }
        )
    )


def test_collect_i64_trailing_window_3s_with_max(source, golden) -> None:
    m = source.col("m")
    golden.jsonl(
        kd.record(
            {
                "m": m,
                "collect_m": m.collect(
                    max=2, window=kd.windows.Trailing(timedelta(seconds=3))
                ),
            }
        )
    )


def test_collect_i64_trailing_window_3s_with_min(source, golden) -> None:
    m = source.col("m")
    golden.jsonl(
        kd.record(
            {
                "m": m,
                "collect_m": m.collect(
                    min=3, max=None, window=kd.windows.Trailing(timedelta(seconds=3))
                ),
            }
        )
    )


def test_collect_string_trailing_window_1s(source, golden) -> None:
    s = source.col("s")
    golden.jsonl(
        kd.record(
            {
                "m": s,
                "collect_s": s.collect(
                    max=None, window=kd.windows.Trailing(timedelta(seconds=1))
                ),
            }
        )
    )


def test_collect_string_trailing_window_3s(source, golden) -> None:
    s = source.col("s")
    golden.jsonl(
        kd.record(
            {
                "s": s,
                "collect_s": s.collect(
                    max=None, window=kd.windows.Trailing(timedelta(seconds=3))
                ),
            }
        )
    )


def test_collect_string_trailing_window_3s_with_max(source, golden) -> None:
    s = source.col("s")
    golden.jsonl(
        kd.record(
            {
                "s": s,
                "collect_s": s.collect(
                    max=2, window=kd.windows.Trailing(timedelta(seconds=3))
                ),
            }
        )
    )


def test_collect_string_trailing_window_3s_with_min(source, golden) -> None:
    s = source.col("s")
    golden.jsonl(
        kd.record(
            {
                "s": s,
                "collect_s": s.collect(
                    min=3, max=None, window=kd.windows.Trailing(timedelta(seconds=3))
                ),
            }
        )
    )


def test_collect_bool_trailing_window_1s(source, golden) -> None:
    b = source.col("b")
    golden.jsonl(
        kd.record(
            {
                "b": b,
                "collect_b": b.collect(
                    max=None, window=kd.windows.Trailing(timedelta(seconds=1))
                ),
            }
        )
    )


def test_collect_bool_trailing_window_3s(source, golden) -> None:
    b = source.col("b")
    golden.jsonl(
        kd.record(
            {
                "b": b,
                "collect_b": b.collect(
                    max=None, window=kd.windows.Trailing(timedelta(seconds=3))
                ),
            }
        )
    )


def test_collect_bool_trailing_window_3s_with_max(source, golden) -> None:
    b = source.col("b")
    golden.jsonl(
        kd.record(
            {
                "b": b,
                "collect_b": b.collect(
                    max=2, window=kd.windows.Trailing(timedelta(seconds=3))
                ),
            }
        )
    )


def test_collect_bool_trailing_window_3s_with_min(source, golden) -> None:
    b = source.col("b")
    golden.jsonl(
        kd.record(
            {
                "b": b,
                "collect_b": b.collect(
                    min=3, max=None, window=kd.windows.Trailing(timedelta(seconds=3))
                ),
            }
        )
    )


# Currently, the Pandas comparison method being used doesn't handle
# date-time like fields nested within a list. So we expand things out.
#
# TODO: Improve the golden testing so this isn't necessary.
def test_collect_struct_trailing_window_1s(source, golden) -> None:
    collect = source.collect(max=None, window=kd.windows.Trailing(timedelta(seconds=1)))
    golden.jsonl(
        kd.record(
            {
                "f0": collect[0].col("time"),
                "f1": collect[1].col("time"),
                "f2": collect[2].col("time"),
                "f3": collect[3].col("time"),
                "f4": collect[4].col("time"),
            }
        )
    )


def test_collect_struct_trailing_window_3s(source, golden) -> None:
    collect = source.collect(max=None, window=kd.windows.Trailing(timedelta(seconds=3)))
    golden.jsonl(
        kd.record(
            {
                "f0": collect[0].col("time"),
                "f1": collect[1].col("time"),
                "f2": collect[2].col("time"),
                "f3": collect[3].col("time"),
                "f4": collect[4].col("time"),
            }
        )
    )


def test_collect_struct_trailing_window_3s_with_max(source, golden) -> None:
    collect = source.collect(max=2, window=kd.windows.Trailing(timedelta(seconds=3)))
    golden.jsonl(
        kd.record(
            {
                "f0": collect[0].col("time"),
                "f1": collect[1].col("time"),
                "f2": collect[2].col("time"),
                "f3": collect[3].col("time"),
                "f4": collect[4].col("time"),
            }
        )
    )


def test_collect_struct_trailing_window_3s_with_min(source, golden) -> None:
    collect = source.collect(
        min=3, max=None, window=kd.windows.Trailing(timedelta(seconds=3))
    )
    golden.jsonl(
        kd.record(
            {
                "f0": collect[0].col("time"),
                "f1": collect[1].col("time"),
                "f2": collect[2].col("time"),
                "f3": collect[3].col("time"),
                "f4": collect[4].col("time"),
            }
        )
    )


def test_collect_records(source, golden) -> None:
    m = source.col("m")
    n = source.col("n")
    golden.jsonl(kd.record({"m": m, "n": n}).collect(max=None))


def test_collect_records_field_ref(source, golden) -> None:
    m = source.col("m")
    n = source.col("n")
    golden.jsonl(kd.record({"m": m, "n": n}).collect(max=None).col("m"))


def test_collect_lists(source, golden) -> None:
    m = source.col("m")
    golden.jsonl(
        kd.record(
            {
                "m": m,
                "list_m": m.collect(max=10),
                "collect_list": m.collect(max=10).collect(max=10),
            }
        )
    )
