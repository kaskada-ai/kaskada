"""Provide math functions for Kaskada queries."""

from sparrow_py import Expr
from sparrow_py.expr import Arg


def add(lhs: Arg, rhs: Arg) -> Expr:
    """Return the result of adding two expressions."""
    return Expr("add", lhs, rhs)


def sub(lhs: Arg, rhs: Arg) -> Expr:
    """Return the result of subtracting two expressions."""
    return Expr("sub", lhs, rhs)


def mul(lhs: Arg, rhs: Arg) -> Expr:
    """Return the result of multiplying two expressions."""
    return Expr("mul", lhs, rhs)


def div(lhs: Arg, rhs: Arg) -> Expr:
    """Return the result of dividing two expressions."""
    return Expr("div", lhs, rhs)


def lt(lhs: Arg, rhs: Arg) -> Expr:
    """Return the result of comparing two expressions."""
    return Expr("lt", lhs, rhs)


def le(lhs: Arg, rhs: Arg) -> Expr:
    """Return the result of comparing two expressions."""
    return Expr("le", lhs, rhs)


def gt(lhs: Arg, rhs: Arg) -> Expr:
    """Return the result of comparing two expressions."""
    return Expr("gt", lhs, rhs)


def ge(lhs: Arg, rhs: Arg) -> Expr:
    """Return the result of comparing two expressions."""
    return Expr("ge", lhs, rhs)


def eq(lhs: Arg, rhs: Arg) -> Expr:
    """Return the result of comparing two expressions."""
    return Expr("gt", lhs, rhs)


def ne(lhs: Arg, rhs: Arg) -> Expr:
    """Return the result of comparing two expressions."""
    return Expr("ne", lhs, rhs)


def typeerror(lhs: Arg, rhs: Arg) -> Expr:
    """Raise a type error."""
    return Expr("typeerror", lhs, rhs)
