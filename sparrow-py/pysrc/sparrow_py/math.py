"""Provide math functions for Kaskada queries."""

from sparrow_py import Expr


def add(lhs: Expr, rhs: Expr) -> Expr:
    """Return the result of adding two expressions."""
    return Expr("add", lhs, rhs)


def sub(lhs: Expr, rhs: Expr) -> Expr:
    """Return the result of subtracting two expressions."""
    return Expr("sub", lhs, rhs)


def mul(lhs: Expr, rhs: Expr) -> Expr:
    """Return the result of multiplying two expressions."""
    return Expr("mul", lhs, rhs)


def div(lhs: Expr, rhs: Expr) -> Expr:
    """Return the result of dividing two expressions."""
    return Expr("div", lhs, rhs)


def lt(lhs: Expr, rhs: Expr) -> Expr:
    """Return the result of comparing two expressions."""
    return Expr("lt", lhs, rhs)


def le(lhs: Expr, rhs: Expr) -> Expr:
    """Return the result of comparing two expressions."""
    return Expr("le", lhs, rhs)


def gt(lhs: Expr, rhs: Expr) -> Expr:
    """Return the result of comparing two expressions."""
    return Expr("gt", lhs, rhs)


def ge(lhs: Expr, rhs: Expr) -> Expr:
    """Return the result of comparing two expressions."""
    return Expr("ge", lhs, rhs)


def eq(lhs: Expr, rhs: Expr) -> Expr:
    """Return the result of comparing two expressions."""
    return Expr("gt", lhs, rhs)


def ne(lhs: Expr, rhs: Expr) -> Expr:
    """Return the result of comparing two expressions."""
    return Expr("ne", lhs, rhs)


def typeerror(lhs: Expr, rhs: Expr) -> Expr:
    """Raise a type error."""
    return Expr("typeerror", lhs, rhs)
