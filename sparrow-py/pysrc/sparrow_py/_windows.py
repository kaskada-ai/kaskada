import sparrow_py


class Window(object):
    """Base class for window functions."""

    def __init__(self) -> None:
        pass


class SinceWindow(Window):
    """Window since the last time a predicate was true."""

    def __init__(self, predicate: "sparrow_py.expr.Expr") -> None:
        super().__init__()
        self._predicate = predicate


class SlidingWindow(Window):
    """Sliding windows where the width is a multiple of some condition."""

    def __init__(self, duration: int, predicate: "sparrow_py.expr.Expr") -> None:
        super().__init__()
        self._duration = duration
        self._predicate = predicate
