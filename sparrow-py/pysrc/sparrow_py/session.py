from sparrow_py import _ffi
from typing import Optional

_SESSION: Optional[_ffi.Session] = None

def init_session() -> None:
    """
    Initialize the Kaskada session for this Python process.

    This must only be called once per session. It must be called before
    any other Kaskada functions are called.

    Parameters
    ----------

    Raises
    ------
    RuntimeError
        If the session has already been initialized.
    """
    global _SESSION
    if _SESSION is not None:
        raise RuntimeError("Session has already been initialized")
    _SESSION = _ffi.Session()

def _get_session() -> _ffi.Session:
    """
    Assert that the session has been initialized.

    Raises
    ------
    AssertionError
        If the session has not been initialized.
    """
    global _SESSION
    assert _SESSION is not None, "Session has not been initialized"
    return _SESSION