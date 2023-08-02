"""PyTest configuration for Sparrow tests."""
import pytest
from sparrow_py import init_session

from typing import Any
from typing import Callable
from typing import Literal
from typing import Union
import pandas as pd
import pytest
import functools
import os
import sparrow_py


@pytest.fixture(autouse=True, scope="session")
def session() -> None:
    """Automatically initialize the session for this PyTest session."""
    init_session()


def pytest_addoption(parser: pytest.Parser):
    parser.addoption("--save-golden", action="store_true", help="update golden files")

@pytest.fixture
def golden(request: pytest.FixtureRequest, pytestconfig: pytest.Config):
    output = 0
    def handler(query: sparrow_py.Expr, format: Union[Literal["csv"], Literal["parquet"]] = "csv"):
        nonlocal output

        df = query.run()

        test_name = request.node.name
        module_name = request.node.module.__name__
        dirname = os.path.join("pytests", "golden", module_name)
        filename = f"{test_name}.{format}" if output == 0 else f"{test_name}_{output}.{format}"
        filename = os.path.join(dirname, filename)
        output += 1

        try:
            save = pytestconfig.getoption("--save-golden")
        except AttributeError as e:
            save = False

        if save:
            os.makedirs(dirname, exist_ok=True)
            if format == "csv":
                df.to_csv(filename, index=False)
            elif format == "parquet":
                df.to_parquet(filename)
            else:
                raise ValueError(f"Unknown format {format}")

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
        else:
            raise ValueError(f"Unknown format {format}")
        pd.testing.assert_frame_equal(df, correct)
    return handler