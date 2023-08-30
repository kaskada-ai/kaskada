import kaskada as kd
import pytest


@pytest.fixture(scope="module")
def source() -> kd.sources.CsvString:
    content = "\n".join(
        [
            "time,key,m",
            "1996-12-19T16:39:57,A,5",
            "1996-12-19T16:39:58,B,100.0001",
            "1996-12-19T16:39:59,A,2.50",
            "1996-12-19T16:40:00,A,",
            "1996-12-19T16:40:01,A,0.99",
            "1996-12-19T16:40:02,A,1.01",
        ]
    )
    return kd.sources.CsvString(content, time_column="time", key_column="key")


def test_clamp_min_max(source, golden) -> None:
    m = source.col("m")
    golden.jsonl(kd.record({"m": m, "clamped_m": m.clamp(min=5, max=100)}))


def test_clamp_min(source, golden) -> None:
    m = source.col("m")
    golden.jsonl(kd.record({"m": m, "clamped_min": m.clamp(min=5)}))


def test_clamp_max(source, golden) -> None:
    m = source.col("m")
    golden.jsonl(kd.record({"m": m, "clamped_min": m.clamp(max=100)}))


@pytest.fixture(scope="module")
def banking_source() -> kd.sources.CsvString:
    content = "\n".join(
        [
            "time,key,current_balance,min_balance,max_balance",
            "1996-12-19T16:39:57,A,5.00,6.00,6.01",
            "1996-12-19T16:39:58,B,6.00,7.00,7.01",
            "1996-12-19T16:39:59,A,7.00,8.00,8.01",
            "1996-12-19T16:40:00,A,8.00,9.00,9.01",
            "1996-12-19T16:40:01,A,9.00,10.00,10.01",
            "1996-12-19T16:40:02,A,10.00,11.00,11.01",
        ]
    )
    return kd.sources.CsvString(content, time_column="time", key_column="key")


def test_clamp_banking_min_max(banking_source, golden) -> None:
    current_balance = banking_source.col("current_balance")
    min_balance = banking_source.col("min_balance")
    max_balance = banking_source.col("max_balance")
    golden.jsonl(
        kd.record(
            {
                "current_balance": current_balance,
                "clamped_balance": current_balance.clamp(
                    min=min_balance, max=max_balance
                ),
            }
        )
    )


def test_clamp_banking_min(banking_source, golden) -> None:
    current_balance = banking_source.col("current_balance")
    min_balance = banking_source.col("min_balance")
    golden.jsonl(
        kd.record(
            {
                "current_balance": current_balance,
                "clamped_balance": current_balance.clamp(min=min_balance),
            }
        )
    )


def test_clamp_banking_max(banking_source, golden) -> None:
    current_balance = banking_source.col("current_balance")
    max_balance = banking_source.col("max_balance")
    golden.jsonl(
        kd.record(
            {
                "current_balance": current_balance,
                "clamped_balance": current_balance.clamp(max=max_balance),
            }
        )
    )
