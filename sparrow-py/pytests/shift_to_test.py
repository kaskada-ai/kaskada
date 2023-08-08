from datetime import timedelta

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
            "1997-01-18T16:40:00-08:00,A,,9",
        ]
    )
    return kt.sources.CsvSource("time", "key", content)


# def test_shift_to_datetime(source, golden) -> None:
#     time = source["time"]
#     shift_to_datetime = datetime(1996, 12, 25, 0, 0, 0, tzinfo=timezone.utc)
#     golden.jsonl(
#         kt.record(
#             {
#                 "time": time,
#                 "shift_to_time_plus_1_day": time.shift_to(shift_to_datetime)
#             }
#         )
#     )


def test_shift_to_column(source, golden) -> None:
    time = source["time"]
    shift_by_timedelta = timedelta(seconds=10)
    golden.jsonl(
        kt.record(
            {
                "time": time,
                "time_plus_seconds": time.shift_to(time.time_of() + shift_by_timedelta),
            }
        )
    )
