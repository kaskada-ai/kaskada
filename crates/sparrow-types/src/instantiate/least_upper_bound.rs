use std::sync::Arc;

use arrow_schema::{DataType, Field};
use hashbrown::HashMap;
use itertools::Itertools;

use crate::{FenlType, TypeConstructor};

pub(super) trait LeastUpperBound: Sized {
    /// Defines a least-upper bound for a pair of values.
    ///
    /// The least-upper bound is partial -- not all pairs have a valid least
    /// upper bound. In this case, the return is `None`.
    fn least_upper_bound(self, other: &Self) -> Option<Self>;
}

impl LeastUpperBound for DataType {
    /// Defines the least-upper-bound of a pair of types.
    ///
    /// This is basically as a 2-d table indicating what happens when two types
    /// are "joined". This is similar to the approach taken in Numpy, Julia,
    /// SQL, C, etc.
    ///
    ///
    /// |     | i8  | i16 | i32 | i64 | u8 | u16 | u32 | u64 | f16 | f32 | f64 |
    /// |-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|
    /// | **i8**  | i8  | i16 | i32 | i64 | i16 | i32 | i64 | **f64**  | f16 | f32 | f64 |
    /// | **i16** | i16 | i16 | i32 | i64 | i16 | i32 | i64 | **f64** | f16 | f32 | f64 |
    /// | **i32** | i32 | i32 | i32 | i64 | i16 | i32 | i64 | **f64** | f16 | f32 | f64 |
    /// | **i64** | i64 | i64 | i64 | i64 | i64 | i64 | i64 | **f64** | f16 | f32 | f64 |
    /// | **u8** | i16 | i16 | i16 | i64 | u8  | u16 | u32 | u64 | f16 | f32 | f64 |
    /// | **u16** | i32 | i32 | i32 | i64 | u16 | u16 | u32 | u64 | f16 | f32 | f64 |
    /// | **u32** | i64 | i64 | i64 | i64 | u32 | u32 | u32 | u64 | f16 | f32 | f64 |
    /// | **u64** | **f64** | **f64** | **f64** | **f64** | u64 | u64 | u64 | u64 | f16 | f32 | f64 |
    /// | **f16** | f16 | f16 | f16 | f16 | f16 | f16 | f16 | f16 | f16 | f32 | f64 |
    /// | **f32** | f32 | f32 | f32 | f32 | f32 | f32 | f32 | f32 | f32 | f32 | f64 |
    /// | **f64** | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 |
    ///
    /// Note that when `u64` is combined with a signed type the result is
    /// `f64`. This is the only case where an operation between integers
    /// produces a floating-point value.
    fn least_upper_bound(self, other: &Self) -> Option<Self> {
        use DataType::*;

        match (self, other) {
            ///////////////////////////////////////////////////////////////////
            // Rules from the numeric table:

            // Row 1: A is i8.
            (Int8, Int8) => Some(Int8),
            (Int8, Int16) => Some(Int16),
            (Int8, Int32) => Some(Int32),
            (Int8, Int64) => Some(Int64),
            (Int8, UInt8) => Some(Int16),
            (Int8, UInt16) => Some(Int32),
            (Int8, UInt32) => Some(Int64),
            (Int8, UInt64) => Some(Float64),
            (Int8, Float16) => Some(Float64),
            (Int8, Float32) => Some(Float64),
            (Int8, Float64) => Some(Float64),
            // Row 2: A is i16.
            (Int16, Int8) => Some(Int16),
            (Int16, Int16) => Some(Int16),
            (Int16, Int32) => Some(Int32),
            (Int16, Int64) => Some(Int64),
            (Int16, UInt8) => Some(Int16),
            (Int16, UInt16) => Some(Int32),
            (Int16, UInt32) => Some(Int64),
            (Int16, UInt64) => Some(Float64),
            (Int16, Float16) => Some(Float64),
            (Int16, Float32) => Some(Float64),
            (Int16, Float64) => Some(Float64),
            // Row 3: A is i32.
            (Int32, Int8) => Some(Int32),
            (Int32, Int16) => Some(Int32),
            (Int32, Int32) => Some(Int32),
            (Int32, Int64) => Some(Int64),
            (Int32, UInt8) => Some(Int32),
            (Int32, UInt16) => Some(Int32),
            (Int32, UInt32) => Some(Int64),
            (Int32, UInt64) => Some(Float64),
            (Int32, Float16) => Some(Float64),
            (Int32, Float32) => Some(Float64),
            (Int32, Float64) => Some(Float64),
            // Row 4: A is i64.
            (Int64, Int8) => Some(Int64),
            (Int64, Int16) => Some(Int64),
            (Int64, Int32) => Some(Int64),
            (Int64, Int64) => Some(Int64),
            (Int64, UInt8) => Some(Int64),
            (Int64, UInt16) => Some(Int64),
            (Int64, UInt32) => Some(Int64),
            (Int64, UInt64) => Some(Float64),
            (Int64, Float16) => Some(Float64),
            (Int64, Float32) => Some(Float64),
            (Int64, Float64) => Some(Float64),
            // Row 5: A is u8.
            (UInt8, Int8) => Some(Int16),
            (UInt8, Int16) => Some(Int16),
            (UInt8, Int32) => Some(Int32),
            (UInt8, Int64) => Some(Int64),
            (UInt8, UInt8) => Some(UInt8),
            (UInt8, UInt16) => Some(UInt16),
            (UInt8, UInt32) => Some(UInt32),
            (UInt8, UInt64) => Some(UInt64),
            (UInt8, Float16) => Some(Float64),
            (UInt8, Float32) => Some(Float64),
            (UInt8, Float64) => Some(Float64),
            // Row 6: A is u16.
            (UInt16, Int8) => Some(Int32),
            (UInt16, Int16) => Some(Int32),
            (UInt16, Int32) => Some(Int32),
            (UInt16, Int64) => Some(Int64),
            (UInt16, UInt8) => Some(UInt16),
            (UInt16, UInt16) => Some(UInt16),
            (UInt16, UInt32) => Some(UInt32),
            (UInt16, UInt64) => Some(UInt64),
            (UInt16, Float16) => Some(Float64),
            (UInt16, Float32) => Some(Float64),
            (UInt16, Float64) => Some(Float64),
            // Row 7: A is u32.
            (UInt32, Int8) => Some(Int64),
            (UInt32, Int16) => Some(Int64),
            (UInt32, Int32) => Some(Int64),
            (UInt32, Int64) => Some(Int64),
            (UInt32, UInt8) => Some(UInt32),
            (UInt32, UInt16) => Some(UInt32),
            (UInt32, UInt32) => Some(UInt32),
            (UInt32, UInt64) => Some(UInt64),
            (UInt32, Float16) => Some(Float64),
            (UInt32, Float32) => Some(Float64),
            (UInt32, Float64) => Some(Float64),
            // Row 8: A is u64.
            (UInt64, Int8) => Some(Int64),
            (UInt64, Int16) => Some(Int64),
            (UInt64, Int32) => Some(Int64),
            (UInt64, Int64) => Some(Int64),
            (UInt64, UInt8) => Some(UInt32),
            (UInt64, UInt16) => Some(UInt32),
            (UInt64, UInt32) => Some(UInt32),
            (UInt64, UInt64) => Some(UInt64),
            (UInt64, Float16) => Some(Float64),
            (UInt64, Float32) => Some(Float64),
            (UInt64, Float64) => Some(Float64),
            // Row 9: A is f16.
            (Float16, Int8) => Some(Float64),
            (Float16, Int16) => Some(Float64),
            (Float16, Int32) => Some(Float64),
            (Float16, Int64) => Some(Float64),
            (Float16, UInt8) => Some(Float64),
            (Float16, UInt16) => Some(Float64),
            (Float16, UInt32) => Some(Float64),
            (Float16, UInt64) => Some(Float64),
            (Float16, Float16) => Some(Float16),
            (Float16, Float32) => Some(Float32),
            (Float16, Float64) => Some(Float64),
            // Row 10: A is f32.
            (Float32, Int8) => Some(Float64),
            (Float32, Int16) => Some(Float64),
            (Float32, Int32) => Some(Float64),
            (Float32, Int64) => Some(Float64),
            (Float32, UInt8) => Some(Float64),
            (Float32, UInt16) => Some(Float64),
            (Float32, UInt32) => Some(Float64),
            (Float32, UInt64) => Some(Float64),
            (Float32, Float16) => Some(Float32),
            (Float32, Float32) => Some(Float32),
            (Float32, Float64) => Some(Float64),
            // Row 11: A is f64.
            (Float64, Int8) => Some(Float64),
            (Float64, Int16) => Some(Float64),
            (Float64, Int32) => Some(Float64),
            (Float64, Int64) => Some(Float64),
            (Float64, UInt8) => Some(Float64),
            (Float64, UInt16) => Some(Float64),
            (Float64, UInt32) => Some(Float64),
            (Float64, UInt64) => Some(Float64),
            (Float64, Float16) => Some(Float64),
            (Float64, Float32) => Some(Float64),
            (Float64, Float64) => Some(Float64),

            // Row 12: A is Utf8
            (Utf8, Utf8) => Some(Utf8),
            (Utf8, ts @ Timestamp(_, _)) => Some(ts.clone()),
            (ts @ Timestamp(_, _), Utf8) => Some(ts),

            //
            ///////////////////////////////////////////////////////////////////
            // Other rules

            // Null is promotable to a null version of any other type.
            (Null, other) => Some(other.clone()),
            (other, Null) => Some(other),

            // Catch all for equal types.
            (lhs, rhs) if &lhs == rhs => Some(lhs),

            // Least Upper bound on records = least upper bound on each field.
            // Currently, this requires the fields to be present (in any order) in both
            // structs. We could relax this and allow absent fields to be treated as null.
            // This could make it easier to make mistakes though -- nulling out one or two
            // fields may be OK, but at some point it may have been intended that the structs
            // were genuinely different.
            (Struct(fields1), Struct(fields2)) => {
                if fields1.len() != fields2.len() {
                    // Early check that the fields don't match.
                    return None;
                }

                // Allow the names to be in any order by creating a map.
                let mut fields2: HashMap<_, _> = fields2
                    .iter()
                    .map(|field2| (field2.name(), field2))
                    .collect();

                // If there is a name in fields1 not present in fields2, then the structs have
                // differing fields.
                if !fields1
                    .iter()
                    .all(|field1| fields2.contains_key(field1.name()))
                {
                    return None;
                }

                // Check to see if we all fields are present and unchanged.
                let unchanged = fields1
                    .iter()
                    .all(|field1| field1.data_type() == fields2[field1.name()].data_type());
                if unchanged {
                    return Some(Struct(fields1));
                }

                // Finally, compute the least-upper-bound.
                let result_fields: Result<Vec<_>, ()> = fields1
                    .iter()
                    .map(|field1| {
                        let field2 = fields2
                            .remove(field1.name())
                            .expect("names lined up earlier");

                        let data_type1 = field1.data_type();
                        let data_type2 = field2.data_type();

                        let data_type =
                            data_type1.clone().least_upper_bound(data_type2).ok_or(())?;

                        if &data_type == data_type1 {
                            Ok(field1.clone())
                        } else if &data_type == data_type2 {
                            Ok(field2.clone())
                        } else {
                            Ok(Arc::new(Field::new(field1.name(), data_type, true)))
                        }
                    })
                    .try_collect();

                if unchanged {
                    Some(Struct(fields1))
                } else if let Ok(fields) = result_fields {
                    Some(Struct(fields.into()))
                } else {
                    None
                }
            }

            // Other types are unrelated.
            (_, _) => None,
        }
    }
}

impl LeastUpperBound for FenlType {
    fn least_upper_bound(self, other: &Self) -> Option<Self> {
        if &self == other {
            Some(self)
        } else {
            match (self.type_constructor, &other.type_constructor) {
                (TypeConstructor::Concrete(lhs), TypeConstructor::Concrete(rhs)) => {
                    lhs.least_upper_bound(rhs).map(FenlType::from)
                }
                _ => None,
            }
        }
    }
}
