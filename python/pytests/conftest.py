import os
from typing import Union

import kaskada as kd
import pandas as pd
import pyarrow as pa
import pytest
from kaskada import init_session


@pytest.fixture(autouse=True, scope="session")
def session() -> None:
    init_session()


def pytest_addoption(parser: pytest.Parser):
    parser.addoption("--save-golden", action="store_true", help="update golden files")


class GoldenFixture(object):
    def __init__(self, dirname: str, test_name: str, save: bool):
        self._output = 0
        self._dirname = dirname
        self._test_name = test_name
        self._save = save

    def jsonl(self, data: Union[kd.Timestream, pd.DataFrame]) -> None:
        """Golden test against newline-delimited JSON file (json-lines)."""
        df = _data_to_dataframe(data)
        filename = self._filename("jsonl")

        if self._save:
            df.to_json(
                filename,
                orient="records",
                lines=True,
                date_format="iso",
                date_unit="ns",
            )

        golden = pd.read_json(
            filename,
            orient="records",
            lines=True,
            dtype=df.dtypes.to_dict(),
            date_unit="ns",
        )

        pd.testing.assert_frame_equal(df, golden, check_datetimelike_compat=True)

    def parquet(self, data: Union[kd.Timestream, pd.DataFrame]) -> None:
        """Golden test against Parquet file."""
        df = _data_to_dataframe(data)
        filename = self._filename("parquet")

        if self._save:
            df.to_parquet(filename)

        golden = pd.read_parquet(filename)

        pd.testing.assert_frame_equal(df, golden)

    def _filename(self, suffix: str) -> str:
        filename = (
            f"{self._test_name}.{suffix}"
            if self._output == 0
            else f"{self._test_name}_{self._output}.{suffix}"
        )
        filename = os.path.join(self._dirname, filename)
        self._output += 1

        if not self._save:
            assert os.path.exists(
                filename
            ), f"Golden file {filename} does not exist. Run with `--save-golden` to create it."
        return filename


def _data_to_dataframe(data: Union[kd.Timestream, pd.DataFrame]) -> pd.DataFrame:
    if isinstance(data, pd.DataFrame):
        return data
    elif isinstance(data, kd.Timestream):
        return data.run().to_pandas()
    else:
        raise ValueError(f"data must be a Timestream or a DataFrame, was {type(data)}")


def _data_to_pyarrow(
    data: Union[kd.Timestream, pa.RecordBatch, pa.Table]
) -> Union[pa.RecordBatch, pa.Table]:
    if isinstance(data, kd.Timestream):
        return data.run().to_pyarrow()
    elif isinstance(data, pa.RecordBatch) or isinstance(data, pa.Table):
        return data
    else:
        raise ValueError(
            f"data must be a Timestream, RecordBatch, or Table, was {type(data)}"
        )


@pytest.fixture
def golden(
    request: pytest.FixtureRequest, pytestconfig: pytest.Config
) -> GoldenFixture:
    """Test fixture for checking results against a golden file."""
    test_name = request.node.name
    module_name = request.node.module.__name__
    dirname = os.path.join("pytests", "golden", module_name)

    save = pytestconfig.getoption("--save-golden", default=False)
    if save:
        os.makedirs(dirname, exist_ok=True)
    else:
        assert os.path.isdir(
            dirname
        ), f"golden directory {dirname} does not exist. run with `--save-golden` to create it."

    return GoldenFixture(dirname, test_name, save)


@pytest.fixture
def source() -> kd.sources.CsvString:
    content = "\n".join(
        [
            "time,key,m,n",
            "1996-12-19T16:39:57,A,5,10",
            "1996-12-19T16:39:58,B,24,3",
            "1996-12-19T16:39:59,A,17,6",
            "1996-12-19T16:40:00,A,,9",
            "1996-12-19T16:40:01,A,12,",
            "1996-12-19T16:40:02,A,,",
        ]
    )
    return kd.sources.CsvString(content, time_column_name="time", key_column_name="key")
