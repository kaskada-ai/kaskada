"""Kaskada query builder and local executon engine."""
from typing import Dict
from typing import Union
from typing import List

from ._windows import SinceWindow
from ._windows import SlidingWindow
from .expr import Expr
from .session import init_session


def record(fields: Dict[str, Expr]) -> Expr:
    """Create a record from the given keyword arguments."""
    import itertools

    args: List[Union[str, "Expr"]] = list(itertools.chain(*fields.items()))
    return Expr.call("record", *args)


__all__ = ["Expr", "init_session", "record", "SinceWindow", "SlidingWindow"]
