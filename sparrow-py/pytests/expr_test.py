"""Tests for the Kaskada query builder."""
from sparrow_py import Literal
from sparrow_py import math


def test_literal() -> None:
    """Test conversion of Python values to literals."""
    lit1 = Literal(5)
    lit2 = Literal(-5)
    lit3 = Literal(5.0)
    lit4 = Literal("Hello")
    assert str(lit1) == "(literal 5u64)"
    assert str(lit2) == "(literal -5i64)"
    assert str(lit3) == "(literal 5f64)"
    assert str(lit4) == '(literal "Hello")'


def test_expr() -> None:
    """Test creating an expression node."""
    literal = Literal(5)
    add2 = math.add(literal, literal)
    assert str(add2) == "(add (literal 5u64) (literal 5u64))"


def test_expr_field_ref() -> None:
    """Test for creating field references."""
    literal = Literal(5)
    field_ref = literal.foo
    assert str(field_ref) == '(fieldref (literal 5u64) (literal "foo"))'


def test_expr_pipe() -> None:
    """Test using `pipe` to create expressions."""
    five = Literal(5)
    six = Literal(6)
    five_lhs = five.pipe(math.add, six)
    assert str(five_lhs) == "(add (literal 5u64) (literal 6u64))"

    six_lhs = five.pipe((math.add, "rhs"), six)
    assert str(six_lhs) == "(add (literal 6u64) (literal 5u64))"

    six_lhs = five.pipe((math.add, "rhs"), lhs=six)
    assert str(six_lhs) == "(add (literal 6u64) (literal 5u64))"
