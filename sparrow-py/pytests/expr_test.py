import sys

import pyarrow as pa
import pytest
import sparrow_py as kt


@pytest.fixture(scope="module")
def source1() -> kt.sources.Source:
    schema = pa.schema(
        [
            pa.field("time", pa.int32(), nullable=False),
            pa.field("key", pa.int64(), nullable=False),
            pa.field("x", pa.float64()),
            pa.field("y", pa.int32()),
        ]
    )
    return kt.sources.Source("time", "key", schema)


def test_field_ref(source1) -> None:
    field_ref_long = source1["x"]
    assert field_ref_long.data_type == pa.float64()


def test_field_ref_no_such_field(source1) -> None:
    with pytest.raises(ValueError, match="Illegal field reference"):
        # This raises a "NoSuchAttribute" error.
        # We currently catch this in Python and don't do anything to
        # suggest possible alternatives.
        #
        # TODO: We should either surface the Sparrow error which suggests
        # possible field names, or improve the Python error.
        source1["foo"]


def test_field_ref_not_a_struct(source1) -> None:
    with pytest.raises(TypeError, match="Cannot index into double"):
        source1["x"]["x"]


def test_expr(source1) -> None:
    x = source1["x"]
    assert x + 1 == x + 1


def test_expr_comparison(source1) -> None:
    x = source1["x"]
    assert (x > 1) == (x > 1)

    # Python doesn't have a `__rgt__` (reverse gt) dunder method.
    # Instead, if the LHS doesn't support `gt` with the RHS, it tries
    # rhs `lt` lhs.
    assert (1 < x) == (x > 1)

    # We can't overload `__eq__` to do this, so we have to use a method.
    x.eq(1)


# def test_expr_pipe(source1) -> None:
#     assert source1.x.pipe(math.add, 1) == math.add(source1.x, 1)
#     assert source1.x.pipe((math.add, "rhs"), 1) == math.add(1, rhs=source1.x)

#     assert source1.x.pipe(math.gt, 1) == math.gt(source1.x, 1)
#     assert source1.x.pipe((math.gt, "rhs"), 1) == math.gt(1, rhs=source1.x)


def test_expr_arithmetic_types(source1) -> None:
    x = source1["x"]
    assert (x.eq(1)).data_type == pa.bool_()
    assert (x + 1).data_type == pa.float64()
    assert (x + source1["y"]).data_type == pa.float64()

    # TODO: This should raise a TypeError, but currently the Rust
    # code always raises a ValueError, so everything comes out
    # looking the same.
    with pytest.raises(ValueError) as e:
        x.eq(1) + source1["y"]
    assert "Incompatible argument types" in str(e)
    if sys.version_info >= (3, 11):
        assert "Arg[0]: Expr of type bool" in e.value.__notes__
        assert "Arg[1]: Expr of type int32" in e.value.__notes__


def test_expr_preview(source1, golden) -> None:
    content = "\n".join(
        [
            "time,key,m,n",
            "1996-12-19T16:39:57-08:00,A,5,10",
            "1996-12-19T16:39:58-08:00,B,24,3",
            "1996-12-19T16:39:59-08:00,A,17,6",
            "1996-12-19T16:40:00-08:00,A,,9",
            "1996-12-19T16:40:01-08:00,A,12,",
            "1996-12-19T16:40:02-08:00,A,,",
        ]
    )
    source = kt.sources.CsvSource("time", "key", content)

    golden(source.preview(limit=4))