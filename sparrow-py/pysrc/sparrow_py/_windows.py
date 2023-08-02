import sparrow_py.expr as expr

class Window(object):
    """Base class for window functions."""
    def __init__(self) -> None:
        pass

class SinceWindow(Window):
    """Window since the last time a predicate was true."""
    def __init__(self, predicate: "expr.Expr") -> None:
        self._predicate = predicate

class SlidingWindow(Window):
    """
    Sliding windows where the width is a multiple of some condition.
    """
    def __init__(self, duration: int, predicate: "expr.Expr") -> None:
        self._duration = duration
        self._predicate = predicate