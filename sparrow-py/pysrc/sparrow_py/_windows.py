from dataclasses import dataclass
from ._expr import Expr

@dataclass(frozen = True)
class Window(object):
    """Base class for window functions."""

@dataclass(frozen = True)
class SinceWindow(Window):
    """
    Window since the last time a predicate was true.

    Aggregations will contain all values starting from the last time the predicate
    evaluated to true (inclusive).

    Parameters
    ----------
    predicate : Expr
        The predicate to use for the window.
        Each time the predicate evaluates to true the window will be cleared.
    """
    predicate: Expr

@dataclass(frozen = True)
class SlidingWindow(Window):
    """
    Sliding windows where the width of the window is determined by the
    number of times (`duration`) the `predicate` is `true`.

    Parameters
    ----------
    duration : int
        The number of sliding intervals to use in the window.

    predicate : Expr
        The predicate to use for the window.
        Each time the predicate evaluates to true the window starts a new interval.
    """

    duration: int
    predicate: Expr