"""Kaskada query builder and local executon engine."""
from typing import Union

from .expr import Expr
from .session import init_session
from ._windows import SinceWindow, SlidingWindow


def record(fields: dict[str, Expr]) -> Expr:
    """Create a record from the given keyword arguments."""
    import itertools

    args: list[Union[str, "Expr"]] = list(itertools.chain(*fields.items()))
    return Expr.call("record", *args)


__all__ = ["Expr", "init_session", "record", "SinceWindow", "SlidingWindow"]
