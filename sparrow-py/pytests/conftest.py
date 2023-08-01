import pytest
from sparrow_py import init_session


@pytest.fixture(autouse = True, scope = "session")
def session() -> None:
    """Automatically initialize the session for this PyTest session."""
    init_session()