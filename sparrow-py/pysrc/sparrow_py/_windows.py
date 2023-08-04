from dataclasses import dataclass

from ._timestream import Timestream


@dataclass(frozen=True)
class Window(object):
    """Base class for window functions."""


@dataclass(frozen=True)
class SinceWindow(Window):
    """
    Window since the last time a predicate was true.

    Aggregations will contain all values starting from the last time the predicate
    evaluated to true (inclusive).

    Parameters
    ----------
    predicate : Timestream
        The boolean Timestream to use as predicate for the window.
        Each time the predicate evaluates to true the window will be cleared.
    """

    predicate: Timestream


@dataclass(frozen=True)
class SlidingWindow(Window):
    """
    Window for the last `duration` intervals of some `predicate`.

    Parameters
    ----------
    duration : int
        The number of sliding intervals to use in the window.

    predicate : Timestream
        The boolean Timestream to use as predicate for the window
        Each time the predicate evaluates to true the window starts a new interval.
    """

    duration: int
    predicate: Timestream
