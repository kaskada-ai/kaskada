import kaskada as kd
import pytest


@pytest.fixture(scope="module")
async def source() -> kd.sources.CsvString:
    content = "\n".join(
        [
            "time,key,json_str",
            '1996-12-19T16:39:57,A,"{\"a\": 10\\, \"b\": \"dog\"}"',
            '1996-12-19T16:39:58,B,"{\"a\": 4\\, \"b\": \"lizard\"}"',
            '1996-12-19T16:39:59,B,"{\"a\": 1\\, \"c\": 3.3}"',
            '1996-12-19T16:40:00,B,"{\"a\": 34}"',
            '1996-12-19T16:40:01,B,"{\"a\": 34}"',
            '1996-12-19T16:40:02,B,"{\"a\": 6\\, \"b\": \"dog\"}"',
        ]
    )
    return await kd.sources.CsvString.create(
        content, time_column="time", key_column="key"
    )

@pytest.fixture(scope="module")
async def invalid_source() -> kd.sources.CsvString:
    content = "\n".join(
        [
            "time,key,json_str",
            '1996-12-19T16:39:57,A,"{a: 10\\, \"b\": \"dog\"}"',
            '1996-12-19T16:39:58,B,"{\"a\": 4\\, \"b: lizard\"}"',
            '1996-12-19T16:39:59,B,"{\"a\": 1\\, \"c\": 3.3}"',
            '1996-12-19T16:40:00,B,"{\"a\": 12\\, \"b\": \"cat\"}"',
            '1996-12-19T16:40:01,B,"{\"a\"\\, 34}"',
            '1996-12-19T16:40:02,B,"{\"a\": 6\\, \"b\": \"dog\"}"',
        ]
    )
    return await kd.sources.CsvString.create(
        content, time_column="time", key_column="key"
    )

# "let json = json(Json.json) in { a_test: json.a as i64, b_test: json(Json.json).b }"
async def test_json_parse_field(source, golden) -> None:
    json_str = source.col("json_str")
    j = json_str.json()
    golden.jsonl(
        kd.record(
            {
                "a_test": j.col("a"),
                "b_test": json_str.json().b,
            }
        )
    )
"""
# "let json = json(Json.json) in { string: json.b, len: len(json.b) }"
async def test_json_string_field_usable_in_string_functions(source, golden) -> None:
    json_str = source.col("json_str")
    j = json_str.json()
    golden.jsonl(
        kd.record(
            {
                "string": j.col("b"),
                "len": j.col("b").len(),
            }
        )
    )

# "let json = json(Json.json) in { num_as_str: json.a as string, len: len(json.a as string) }"
async def test_json_field_number_as_string(source, golden) -> None:
    json_str = source.col("json_str")
    j = json_str.json()
    golden.jsonl(
        kd.record(
            {
                "num_as_str": j.col("a").cast("str"),
                "len": j.col("a").cast("str").len(),
            }
        )
    )

# "let json = json(Json.json) in { a: json.a, plus_one: (json.a as i64) + 1 }"
async def test_json_field_as_number_with_addition(source, golden) -> None:
    json_str = source.col("json_str")
    j = json_str.json()
    golden.jsonl(
        kd.record(
            {
                "a": j.col("a"),
                "plus_one": j.col("a").cast("i64") + 1,
            }
        )
    )

# "let json = json(Json.json) in { a_test: json.a as i64, b_test: json(Json.json).b }"
#
# I guess this behavior is somewhat strange, in that creating a record with all
# nulls produces nothing, while one non-null field in a record causes us to
# print "null" in other fields.
async def test_incorrect_json_format_produces_null(invalid_source, golden) -> None:
    json_str = invalid_source.col("json_str")
    j = json_str.json()
    golden.jsonl(
        kd.record(
            {
                "a_test": j.col("a").cast("i64"),
                "b_test": json_str.json().b,
            }
        )
    )

# "let json = json(Json.json) in { a: json(json) }"
async def test_json_of_json_object_errors(invalid_source, golden) -> None:
    json_str = invalid_source.col("json_str")
    j = json_str.json()
    golden.jsonl(
        kd.record(
            {
                "a": j.json(),
            }
        )
    )

# "{ out: json(Json.json).a.b }"
#
# There's a way we can probably produce a better error message,
# but, on the other hand, it's marked as an experimental feature, plus
# this returns an error rather than incorrect results :shrug:
#
# The `dfg` would need to check if it recursively encounters the pattern
# `(field_ref (json ?value ?op) ?field ?op)`
async def test_nested_json_produces_error(invalid_source, golden) -> None:
    json_str = invalid_source.col("json_str")
    j = json_str.json()
    golden.jsonl(
        kd.record(
            {
                "out": j.json().col("a").col("b"),
            }
        )
    )

# \"{ out: json(Json.json) }"
async def test_json_as_output_field_produces_error(invalid_source, golden) -> None:
    golden.jsonl(
        kd.record(
            {
                "out": invalid_source.col("json_str").json(),
            }
        )
    )
 """