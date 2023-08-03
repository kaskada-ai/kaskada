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


@pytest.fixture
def golden(request: pytest.FixtureRequest, pytestconfig: pytest.Config):  # noqa: C901
    """Test fixture for checking results against a golden file."""
    output = 0

    def handler(
        query: Union[kt.Timestream, pd.DataFrame],
        format: Union[Literal["csv"], Literal["parquet"], Literal["json"]] = "json",
    ):
        """
        Check query results against a golden file.

        Parameters
        ----------
        query : kt.Timestream to execute or pd.DataFrame
            The query to run (or a result to use).
        format : str, optional
            The format to store the golden file in.
            Defaults to "json".

        Raises
        ------
        ValueError
            If the `format` is not recognized.
        """
        nonlocal output

        if isinstance(query, pd.DataFrame):
            df = query
        elif isinstance(query, kt.Timestream):
            df = query.run().to_pandas()
        else:
            raise ValueError(
                f"query must be a Timestream or a DataFrame, was {type(query)}"
            )

        test_name = request.node.name
        module_name = request.node.module.__name__
        dirname = os.path.join("pytests", "golden", module_name)
        filename = (
            f"{test_name}.{format}" if output == 0 else f"{test_name}_{output}.{format}"
        )
        filename = os.path.join(dirname, filename)
        output += 1

        save = pytestconfig.getoption("--save-golden", default=False)

        if save:
            os.makedirs(dirname, exist_ok=True)
            if format == "csv":
                df.to_csv(filename, index=False)
            elif format == "parquet":
                df.to_parquet(filename)
            elif format == "json":
                df.to_json(filename, orient="records", lines=True, date_unit="ns")
            else:
                raise ValueError(f"Unknown format {format}")
        else:
            assert os.path.exists(
                filename
            ), f"Golden file {filename} does not exist. Run with `--save-golden` to create it."

        if format == "csv":
            dtypes = {}
            parse_dates = []
            for name, dtype in df.dtypes.to_dict().items():
                if pd.api.types.is_datetime64_dtype(dtype):
                    parse_dates.append(name)
                else:
                    dtypes[name] = dtype
            correct = pd.read_csv(filename, dtype=dtypes, parse_dates=parse_dates)
        elif format == "parquet":
            correct = pd.read_parquet(filename)
        elif format == "json":
            correct = pd.read_json(
                filename,
                orient="records",
                lines=True,
                dtype=df.dtypes.to_dict(),
                date_unit="ns",
            )
        else:
            raise ValueError(f"Unknown format {format}")
        pd.testing.assert_frame_equal(df, correct)

    return handler
