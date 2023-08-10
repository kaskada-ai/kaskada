import os
from typing import Union

import pandas as pd
import pyarrow as pa
import pytest
import sparrow_py as kt
from sparrow_py import init_session


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

    def csv(self, data: Union[kt.Timestream, pd.DataFrame]) -> None:
        """
        Golden test against CSV file.

        Uses Pyarrow to save and load the CSV file. The schema of the `data`
        is used to align types.
        """
        data = _data_to_pyarrow(data)
        filename = self._filename("csv")

        # Save the CSV file, if requested.
        if self._save:
            pa.csv.write(data, filename)

        # Load the CSV file. Use the schema of the data to set expected types.
        golden = pa.read_csv(filename,
            convert_options = pa.csv.ConvertOptions(column_types=data.schema))

        pd.testing.assert_frame_equal(df, golden)

    def jsonl(self, data: Union[kt.Timestream, pd.DataFrame]) -> None:
        """Golden test against newline-delimited JSON file (json-lines)."""
        df = _data_to_dataframe(data)
        filename = self._filename("jsonl")

        if self._save:
            df.to_json(filename, orient="records", lines=True, date_format="iso")

        golden = pd.read_json(
            filename,
            orient="records",
            lines=True,
            dtype=df.dtypes.to_dict(),
            date_unit="ns",
        )

        pd.testing.assert_frame_equal(df, golden, check_datetimelike_compat=True)

    def parquet(self, data: Union[kt.Timestream, pd.DataFrame]) -> None:
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


def _data_to_dataframe(data: Union[kt.Timestream, pd.DataFrame]) -> pd.DataFrame:
    if isinstance(data, pd.DataFrame):
        return data
    elif isinstance(data, kt.Timestream):
        return data.run().to_pandas()
    else:
        raise ValueError(f"data must be a Timestream or a DataFrame, was {type(data)}")

def _data_to_pyarrow(data: Union[kt.Timestream, pa.RecordBatch, pa.Table]) -> Union[pa.RecordBatch, pa.Table]:
    if isinstance(data, kt.Timsetream):
        return data.run().to_pyarrow()
    elif isinstance(data, pa.RecordBatch) or isinstance(data, pa.Table):
        return data
    else:
        raise ValueError(f"data must be a Timestream, RecordBatch, or Table, was {type(data)}")

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
