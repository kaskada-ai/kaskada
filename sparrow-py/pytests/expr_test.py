"""Tests for the Kaskada query builder."""
import sys

import pyarrow as pa
import pytest
from sparrow_py import Table
from sparrow_py import math


@pytest.fixture(scope = "module")
def table1() -> Table:
    """Create a table for testing."""
    schema = pa.schema(
        [
            pa.field("time", pa.int32(), nullable=False),
            pa.field("key", pa.int64(), nullable=False),
            pa.field("x", pa.float64()),
            pa.field("y", pa.int32()),
        ]
    )
    return Table("time", "key", schema)


def test_field_ref(table1) -> None:
    """Test for field references."""
    field_ref_short = table1.x
    assert field_ref_short.data_type == pa.float64()
    assert field_ref_short == table1.x

    field_ref_long = table1["x"]
    assert field_ref_long.data_type == pa.float64()
    assert field_ref_short == field_ref_long


def test_field_ref_no_such_field(table1) -> None:
    """Test error when there is no such field."""
    with pytest.raises(AttributeError) as e:
        # This raises a "NoSuchAttribute" error.
        # We currently catch this in Python and don't do anything to
        # suggest possibly alternatives.
        #
        # TODO: We should either surface the Sparrow error which suggests
        # possible field names, or improve the Python error.
        table1.foo
    assert "Field 'foo' not found in 'time', 'key', 'x', 'y'" == str(e.value)


def test_field_ref_not_a_struct(table1) -> None:
    """Test error when there the base is not a struct."""
    with pytest.raises(TypeError) as e:
        table1.x.x
    assert "Cannot access field 'x' on non-struct type 12" == str(e.value)


def test_expr(table1) -> None:
    """Test creating an expression node."""
    x = table1.x
    assert x + 1 == x + 1


def test_expr_comparison(table1) -> None:
    """Test basic comparisons."""
    assert (table1.x > 1) == (table1.x > 1)

    # Python doesn't have a `__rgt__` (reverse gt) dunder method.
    # Instead, if the LHS doesn't support `gt` with the RHS, it tries
    # rhs `lt` lhs.
    assert (1 < table1.x) == (table1.x > 1)


def test_expr_pipe(table1) -> None:
    """Test using `pipe` to create expressions."""
    assert table1.x.pipe(math.add, 1) == math.add(table1.x, 1)
    assert table1.x.pipe((math.add, "rhs"), 1) == math.add(1, rhs=table1.x)

    assert table1.x.pipe(math.gt, 1) == math.gt(table1.x, 1)
    assert table1.x.pipe((math.gt, "rhs"), 1) == math.gt(1, rhs=table1.x)


def test_expr_arithmetic_types(table1) -> None:
    """Test type inference and type errors of arithmetic expressions."""
    assert math.eq(table1.x, 1).data_type == pa.bool_()
    assert math.add(table1.x, 1).data_type == pa.float64()
    assert (table1.x + table1.y).data_type == pa.float64()

    # TODO: This should raise a TypeError, but currently the Rust
    # code always raises a ValueError, so everything comes out
    # looking the same.
    with pytest.raises(ValueError) as e:
        math.eq(table1.x, 1) + table1.y
    assert "Incompatible argument types" in str(e)
    if sys.version_info >= (3, 11):
        assert "Arg[0]: Expr of type bool" in e.value.__notes__
        assert "Arg[1]: Expr of type int32" in e.value.__notes__
