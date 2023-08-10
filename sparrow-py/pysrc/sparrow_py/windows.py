from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta

from ._timestream import Timestream


@dataclass(frozen=True)
class Window(object):
    """Base class for window functions."""


@dataclass(frozen=True)
class Since(Window):
    """
    Window since the last time a predicate was true.

    Aggregations will contain all values starting from the last time the predicate
    evaluated to true (inclusive).

    Parameters
    ----------
    predicate : Timestream | bool
        The boolean Timestream to use as predicate for the window.
        Each time the predicate evaluates to true the window will be cleared.
    """

    predicate: Timestream | bool


@dataclass(frozen=True)
class Sliding(Window):
    """
    Window for the last `duration` intervals of some `predicate`.

    Parameters
    ----------
    duration : int
        The number of sliding intervals to use in the window.

    predicate : Timestream | bool
        The boolean Timestream to use as predicate for the window
        Each time the predicate evaluates to true the window starts a new interval.
    """

    duration: int
    predicate: Timestream | bool

    def __post_init__(self):
        if self.duration <= 0:
            raise ValueError("duration must be positive")


@dataclass(frozen=True)
class Trailing(Window):
    """
    Window the last `duration` time period.

    Parameters
    ----------
    duration : timedelta
        The duration of the window.
    """

    duration: timedelta

    def __post_init__(self):
        if self.duration <= timedelta(0):
            raise ValueError("duration must be positive")
