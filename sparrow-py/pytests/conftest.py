import os
from typing import Literal
from typing import Union

import pandas as pd
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

        Uses Pandas to save and load the CSV file. The schema of the `data`
        is used to align types.
        """
        df = _data_to_dataframe(data)
        filename = self._filename("csv")

        # Save the CSV file, if requested.
        if self._save:
            df.to_csv(filename, index=False)

        # Load the CSV file. Use the schema of the data to set expected types.
        dtypes = {}
        parse_dates = []
        for name, dtype in df.dtypes.to_dict().items():
            if pd.api.types.is_datetime64_dtype(dtype):
                parse_dates.append(name)
            else:
                dtypes[name] = dtype
        golden = pd.read_csv(filename, dtype=dtypes, parse_dates=parse_dates)

        pd.testing.assert_frame_equal(df, golden)

    def jsonl(self, data: Union[kt.Timestream, pd.DataFrame]) -> None:
        """Golden test against JSON file."""
        df = _data_to_dataframe(data)
        filename = self._filename("jsonl")

        if self._save:
            df.to_json(filename, orient="records", lines=True, date_unit="ns")

        golden = pd.read_json(
            filename,
            orient="records",
            lines=True,
            dtype=df.dtypes.to_dict(),
            date_unit="ns",
        )

        pd.testing.assert_frame_equal(df, golden)

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
            f"{self._test_name}.{suffix}" if self._output == 0 else f"{self._test_name}_{self._output}.{suffix}"
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
        raise ValueError(
            f"query must be a Timestream or a DataFrame, was {type(query)}")

@pytest.fixture
def golden(request: pytest.FixtureRequest, pytestconfig: pytest.Config) -> GoldenFixture:
    """Test fixture for checking results against a golden file."""
    test_name = request.node.name
    module_name = request.node.module.__name__
    dirname = os.path.join("pytests", "golden", module_name)

    save = pytestconfig.getoption("--save-golden", default=False)
    if save:
        os.makedirs(dirname, exist_ok=True)
    else:
        assert os.path.isdir(dirname), f"golden directory {dirname} does not exist. run with `--save-golden` to create it."

    return GoldenFixture(dirname, test_name, save)