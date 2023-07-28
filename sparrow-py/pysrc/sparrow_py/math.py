"""Provide math functions for Kaskada queries."""

from sparrow_py import Expr
from sparrow_py.expr import Arg


def add(lhs: Arg, rhs: Arg) -> Expr:
    """Return the result of adding two expressions."""
    return Expr.call("add", lhs, rhs)


def sub(lhs: Arg, rhs: Arg) -> Expr:
    """Return the result of subtracting two expressions."""
    return Expr.call("sub", lhs, rhs)


def mul(lhs: Arg, rhs: Arg) -> Expr:
    """Return the result of multiplying two expressions."""
    return Expr.call("mul", lhs, rhs)


def div(lhs: Arg, rhs: Arg) -> Expr:
    """Return the result of dividing two expressions."""
    return Expr.call("div", lhs, rhs)


def lt(lhs: Arg, rhs: Arg) -> Expr:
    """Return the result of comparing two expressions."""
    return Expr.call("lt", lhs, rhs)


def le(lhs: Arg, rhs: Arg) -> Expr:
    """Return the result of comparing two expressions."""
    return Expr.call("le", lhs, rhs)


def gt(lhs: Arg, rhs: Arg) -> Expr:
    """Return the result of comparing two expressions."""
    return Expr.call("gt", lhs, rhs)


def ge(lhs: Arg, rhs: Arg) -> Expr:
    """Return the result of comparing two expressions."""
    return Expr.call("ge", lhs, rhs)


def eq(lhs: Arg, rhs: Arg) -> Expr:
    """Return the result of comparing two expressions."""
    return Expr.call("gt", lhs, rhs)


def ne(lhs: Arg, rhs: Arg) -> Expr:
    """Return the result of comparing two expressions."""
    return Expr.call("ne", lhs, rhs)


def typeerror(lhs: Arg, rhs: Arg) -> Expr:
    """Raise a type error."""
    return Expr.call("typeerror", lhs, rhs)
