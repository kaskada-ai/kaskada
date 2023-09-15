# Data Types

Kaskada operates on typed Timestreams.
Similar to how every Pandas `DataFrame` has an associated `dtype`, every Kaskada `Timestream` has an associated [PyArrow data type](https://arrow.apache.org/docs/python/api/datatypes.html) returned by {py:attr}`kaskada.Timestream.data_type`.
The set of supported types is based on the types supported by [Apache Arrow](https://arrow.apache.org/).

Each `Timestream` contains points of the corresponding type.
We'll often say that the "type" of a `Timestream` is the type of the values it contains.

Kaskada's type system describes several kinds of values.
Scalar types correspond to simple values, such as the string `"hello"` or the integer `57`.
They correspond to a stream containing values of the given type, or `null`.
Composite types are created from other types.
For instance, records may be created using scalar and other composite types as fields.
An expression producing a record type is a stream that produces a value of the given record type or `null`.

## Scalar Types

Scalar types include booleans, numbers, strings, timestamps, durations and calendar intervals.

:::{list-table} Scalar Types
:widths: 1, 3
:header-rows: 1

- * Types
  * Description
- * `bool`
  * Booleans represent true or false.

    Examples: `true`, `false`.
- * `u8`, `u16`, `u32`, `u64`
  * Unsigned integer numbers of the specified bit width.

    Examples: `0`, `1`, `1000`
- * `i8`, `i16`, `i32`, `i64`
  * Signed integer numbers of the specified bit width.

    Examples: `0`, `1`, `-100`
- * `f32`, `f64`
  * Floating point numbers of the specified bit width.

    Examples: `0`, `1`, `-100`, `1000`, `0.0`, `-1.0`, `-100837.631`.
- * `str`
  * Unicode strings.

    Examples: `"hello", "hi 'bob'"`.

- * `timestamp_s`, `timestamp_ms`, `timestamp_us`, `timestamp_ns`
  * Points in time relative the Unix Epoch (00:00:00 UTC on January 1, 1970).
    Time unit may be seconds (s), milliseconds (ms), microseconds (us) or nanoseconds (ns).

    Examples: `1639595174 as timestamp_s`
- *  `duration_s`, `duration_ms`, `duration_us`, `duration_ns`
  * A duration of a fixed amount of a specific time unit.
    Time unit may be seconds (s), milliseconds (ms), microseconds (us) or nanoseconds (ns).

    Examples: `-100 as duration_ms`
- * `interval_days`, `interval_months`
  * A calendar interval corresponding to the given amount of the corresponding time.
    The length of an interval depends on the point in time it is added to.
    For instance, adding 1 `interval_month` to a timestamp will shift to the same day of the next month.

    Examples: `1 as interval_days`, `-100 as interval_months`
:::

## Record Types

Records allow combining 1 or more values of potentially different types into a single value.
Records are unnamed - any two records with the same set of field names and value types are considered equal. Fields within a record may have different types.
Field names must start with a letter.

For example, `{name: string, age: u32 }` is a record type with two fields and `{name: 'Ben', age: 33 }` is corresponding value.

NOTE: Record types may be nested.

## Collection Types

Kaskada also supports collections -- lists and maps.

The type `list<T>` describes a list of elements of type `T`.
For example, `list<i64>` is a list of 64-bit integers.

Similarly, `map<K, V>` describes a map containing keys of type `K` and values of type `V`.
For example, `map<str, i64>` is a map from strings to 64-bit integers.

## Type Coercion
Kaskada implicitly coerces numeric types when different kinds of numbers are combined.
For example adding a 64-bit signed integer value to a 32-bit floating point value produces a 64-point floating point value

Type coercion will never produce an integer overflow or reduction in numeric precision.
If needed, such conversions must be explicitly specified using `as`.

The coercion rules can be summarized with the following rules:

1. Unsigned integers can be widened: `u8` ⇨ `u16` ⇨ `u32` ⇨ `u64`.
2. Integers can be widened: `i8` ⇨ `i16` ⇨ `i32` ⇨ `i64`.
3. Floating point numbers can be widened: `f16` ⇨ `f32` ⇨ `f64`.
4. Unsigned integers can be promoted to the next wider integer `u8` ⇨ `i16`, `u16` ⇨ `i32`, `u32` ⇨ `i64`.
5. All numbers may be converted to `f64`.
6. Strings may be implicitly converted to timestamps by attempting to parse them as RFC3339 values.
The timestamp will be null for strings that don't successfully parse.

One aspect of the coercion rules is that when an operation is applied to two different numeric types the result may be a third type which they may both be coerced to.
The type promotion table shows the type resulting from a binary operation involving two different numeric types.

|           |  `u8` | `u16` | `u32` | `u64` | `i8`  | `i16` | `i32` | `i64` | `f16` | `f32` | `f64` |
| --------- | ----- | ----- | ----- | ----- | ----- | ----- | ----- | ----- | ----- | ----- | ----- |
| **`u8`**  |  `u8` | `u16` | `u32` | `u64` | `i16` | `i16` | `i32` | `i64` | `f16` | `f32` | `f64` |
| **`u16`** | `u16` | `u16` | `u32` | `u64` | `i32` | `i32` | `i32` | `i64` | `f16` | `f32` | `f64` |
| **`u32`** | `u32` | `u32` | `u32` | `u64` | `i64` | `i64` | `i64` | `i64` | `f32` | `f32` | `f64` |
| **`u64`** | `u64` | `u64` | `u64` | `u64` | `f64` | `f64` | `f64` | `f64` | `f64` | `f64` | `f64` |
| **`i8`**  | `i16` | `i32` | `i64` | `f64` | `i8`  | `i16` | `i32` | `i64` | `f16` | `f32` | `f64` |
| **`i16`** | `i16` | `i32` | `i64` | `f64` | `i16` | `i16` | `i32` | `i64` | `f16` | `f32` | `f64` |
| **`i32`** | `i32` | `i32` | `i64` | `f64` | `i32` | `i32` | `i32` | `i64` | `f16` | `f32` | `f64` |
| **`i64`** | `i64` | `i64` | `i64` | `f64` | `i64` | `i64` | `i64` | `i64` | `f16` | `f32` | `f64` |
| **`f16`** | `f16` | `f16` | `f16` | `f16` | `f16` | `f16` | `f16` | `f16` | `f16` | `f32` | `f64` |
| **`f32`** | `f32` | `f32` | `f32` | `f32` | `f32` | `f32` | `f32` | `f32` | `f32` | `f32` | `f64` |
| **`f64`** | `f64` | `f64` | `f64` | `f64` | `f64` | `f64` | `f64` | `f64` | `f64` | `f64` | `f64` |