"""Kaskada query builder and local executon engine."""
from __future__ import annotations

from . import sources
from ._execution import ExecutionOptions
from ._result import Result
from ._session import init_session
from ._timestream import Literal
from ._timestream import Timestream
from ._timestream import record
from ._windows import SinceWindow
from ._windows import SlidingWindow
from ._windows import Window


__all__ = [
    "ExecutionOptions",
    "init_session",
    "Literal",
    "record",
    "Result",
    "SinceWindow",
    "SlidingWindow",
    "sources",
    "Timestream",
    "Window",
]
