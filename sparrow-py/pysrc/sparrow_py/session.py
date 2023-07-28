from sparrow_py import _ffi


class Session(object):
    """A Kaskada session."""

    _ffi: _ffi.Session

    def __init__(self) -> None:
        """Create a new session."""
        self._ffi = _ffi.Session()
