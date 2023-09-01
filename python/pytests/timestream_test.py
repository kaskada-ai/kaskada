import sys

import kaskada as kd
import pyarrow as pa
import pytest


@pytest.fixture(scope="module")
def source1() -> kd.sources.Source:
    schema = pa.schema(
        [
            pa.field("time", pa.int32(), nullable=False),
            pa.field("key", pa.int64(), nullable=False),
            pa.field("x", pa.float64()),
            pa.field("y", pa.int32()),
        ]
    )
    return kd.sources.Source(schema, time_column="time", key_column="key")


def test_field_ref(source1) -> None:
    field_ref_long = source1.col("x")
    assert field_ref_long.data_type == pa.float64()


def test_field_ref_no_such_field(source1) -> None:
    with pytest.raises(ValueError, match="Illegal field reference"):
        # This raises a "NoSuchAttribute" error.
        # We currently catch this in Python and don't do anything to
        # suggest possible alternatives.
        #
        # TODO: We should either surface the Sparrow error which suggests
        # possible field names, or improve the Python error.
        source1.col("foo")


def test_field_ref_not_a_struct(source1) -> None:
    with pytest.raises(
        TypeError, match="Cannot access column 'x' of non-record type 'double'"
    ):
        source1.col("x").col("x")


def test_timestream_math(source1) -> None:
    x = source1.col("x")
    assert (x + 1).data_type == x.data_type
    assert (1 + x).data_type == x.data_type
    assert (x - 1).data_type == x.data_type
    assert (1 - x).data_type == x.data_type
    assert (x * 1).data_type == x.data_type
    assert (1 * x).data_type == x.data_type
    assert (1 / x).data_type == x.data_type
    assert (1 + x).data_type == x.data_type

    assert (1 + x).neg().data_type == x.data_type


def test_timestream_comparison(source1) -> None:
    x = source1.col("x")

    # Tests the various comparison operators. Even though Python doesn't have a
    # `__rgt__` (reverse gt) dunder method, if the LHS doesn't support `gt` with
    # the RHS it seems to try `rhs lt lhs`.
    assert (x > 1).data_type == pa.bool_()
    assert (1 > x).data_type == pa.bool_()
    assert (x < 1).data_type == pa.bool_()
    assert (1 < x).data_type == pa.bool_()
    assert (x >= 1).data_type == pa.bool_()
    assert (1 >= x).data_type == pa.bool_()
    assert (x <= 1).data_type == pa.bool_()
    assert (1 <= x).data_type == pa.bool_()

    # For `eq` and `ne` we only support timestream on the LHS since it is a method.
    # We can't overload `__eq__` since that must take any RHS and must return `bool`.
    assert x.eq(1).data_type == pa.bool_()
    assert x.ne(1).data_type == pa.bool_()

    a = x > 1
    b = x.eq(1)
    assert a.not_().data_type == pa.bool_()
    assert a.and_(b).data_type == pa.bool_()
    assert a.or_(b).data_type == pa.bool_()


def test_timestream_eq_warning(source1) -> None:
    x = source1.col("x")
    y = source1.col("y")

    with pytest.warns(UserWarning, match="Use 'eq'"):
        x == y  # noqa : B015

    with pytest.warns(UserWarning, match="Use 'ne'"):
        x != y  # noqa : B015


def test_timestream_arithmetic_types(source1) -> None:
    x = source1.col("x")
    assert (x.eq(1)).data_type == pa.bool_()
    assert (x + 1).data_type == pa.float64()
    assert (x + source1.col("y")).data_type == pa.float64()

    # TODO: This should raise a TypeError, but currently the Rust
    # code always raises a ValueError, so everything comes out
    # looking the same.
    with pytest.raises(ValueError) as e:
        x.eq(1) + source1.col("y")
    assert "Incompatible argument types" in str(e)
    if sys.version_info >= (3, 11):
        assert "Arg[0]: Timestream[bool]" in e.value.__notes__
        assert "Arg[1]: Timestream[int32]" in e.value.__notes__


def test_timestream_preview(golden) -> None:
    content = "\n".join(
        [
            "time,key,m,n",
            "1996-12-19T16:39:57,A,5,10",
            "1996-12-19T16:39:58,B,24,3",
            "1996-12-19T16:39:59,A,17,6",
            "1996-12-19T16:40:00,A,,9",
            "1996-12-19T16:40:01,A,12,",
            "1996-12-19T16:40:02,A,,",
        ]
    )
    source = kd.sources.CsvString(content, time_column="time", key_column="key")

    golden.jsonl(source.preview(limit=4))


def test_timestream_run_non_record(golden) -> None:
    content = "\n".join(
        [
            "time,key,m,n",
            "1996-12-19T16:39:57,A,5,10",
            "1996-12-19T16:39:58,B,24,3",
            "1996-12-19T16:39:59,A,17,6",
            "1996-12-19T16:40:00,A,,9",
            "1996-12-19T16:40:01,A,12,",
            "1996-12-19T16:40:02,A,,",
        ]
    )
    source = kd.sources.CsvString(content, time_column="time", key_column="key")
    golden.jsonl(source.col("m"))


def test_timestream_cast(golden) -> None:
    content = "\n".join(
        [
            "time,key,m,n",
            "1996-12-19T16:39:57,A,5,10",
            "1996-12-19T16:39:58,B,24,3",
            "1996-12-19T16:39:59,A,17,6",
            "1996-12-19T16:40:00,A,,9",
            "1996-12-19T16:40:01,A,12,",
            "1996-12-19T16:40:02,A,,",
        ]
    )
    source = kd.sources.CsvString(content, time_column="time", key_column="key")
    golden.jsonl(source.col("time").cast(pa.timestamp("ns")))


def test_timestream_call_all_literals() -> None:
    with pytest.raises(ValueError, match="at least one non-literal"):
        kd.Timestream._call("foo", 1, 2, 3)
