import random

import pandas as pd
import sparrow_py as kt


def test_add_dataframe(golden) -> None:
    random.seed(1000)
    member_ids = list(range(0, 10))
    records = []
    for member_id in member_ids:
        for _i in range(0, 10):
            records.append(
                {
                    # number of seconds from epoch
                    "time": random.randint(1000, 4000) * 1000000000000,
                    "key": member_id,
                }
            )
    dataset1 = pd.DataFrame(records)

    table = kt.sources.Pandas(dataset1, time_column_name="time", key_column_name="key")
    golden.jsonl(table)

    records.clear()
    for member_id in member_ids:
        for _i in range(0, 10):
            records.append(
                {
                    # number of seconds from epoch
                    "time": random.randint(3000, 7000) * 1000000000000,
                    "key": member_id,
                }
            )
    dataset2 = pd.DataFrame(records)
    table.add_data(dataset2)
    golden.jsonl(table)
