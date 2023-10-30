use anyhow::Context;
use arrow::array::{
    Array, ArrayData, ArrayRef, BooleanArray, Int32Builder, UInt32Array, UInt32Builder,
};
use arrow::datatypes::DataType;
use sparrow_instructions::GroupingIndices;

#[repr(transparent)]
#[derive(Debug)]
pub struct Spread {
    spread_impl: Box<dyn SpreadImpl>,
}

impl serde::Serialize for Spread {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let e = self.spread_impl.to_serialized_spread();
        e.serialize(serializer)
    }
}

impl<'de> serde::Deserialize<'de> for Spread {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let e = SerializedSpread::deserialize(deserializer)?;
        let Some(spread_impl) = e.into_spread_impl() else {
            use serde::de::Error;
            return Err(D::Error::custom("expected owned"));
        };

        Ok(Self { spread_impl })
    }
}

// A borrowed-or-owned `T`.
#[derive(Debug)]
enum Boo<'a, T> {
    Borrowed(&'a T),
    Owned(T),
}

impl<'a, T: serde::Serialize> serde::Serialize for Boo<'a, T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            Boo::Borrowed(t) => (*t).serialize(serializer),
            Boo::Owned(t) => t.serialize(serializer),
        }
    }
}

impl<'a, 'de, T: serde::Deserialize<'de>> serde::Deserialize<'de> for Boo<'a, T> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        T::deserialize(deserializer).map(Self::Owned)
    }
}

trait ToSerializedSpread {
    fn to_serialized_spread(&self) -> SerializedSpread<'_>;
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
enum SerializedSpread<'a> {
    Latched(Boo<'a, LatchedSpread>),
    Unlatched(Boo<'a, UnlatchedSpread>),
}

fn into_spread_impl<T: SpreadImpl + 'static>(spread: Boo<'_, T>) -> Option<Box<dyn SpreadImpl>> {
    match spread {
        Boo::Borrowed(_) => None,
        Boo::Owned(spread) => Some(Box::new(spread)),
    }
}

impl<'a> SerializedSpread<'a> {
    fn into_spread_impl(self) -> Option<Box<dyn SpreadImpl>> {
        match self {
            SerializedSpread::Latched(spread) => into_spread_impl(spread),
            SerializedSpread::Unlatched(spread) => into_spread_impl(spread),
        }
    }
}

impl Spread {
    /// Return the implementation of `spread`.
    ///
    /// If `latched` is true, the implementation will use a "latched" version of
    /// the spread logic, which will remember the previously output value
    /// for each key. This ensures that continuity is maintained.
    pub(super) fn try_new(latched: bool, data_type: &DataType) -> anyhow::Result<Spread> {
        let spread_impl: Box<dyn SpreadImpl> = if latched {
            Box::new(LatchedSpread::new(data_type))
        } else {
            Box::new(UnlatchedSpread)
        };

        Ok(Self { spread_impl })
    }

    /// Spread the values out according to the given signal.
    ///
    /// The result will have the same length as `signal`. For each row,
    /// if `signal` is true then the next value from `values` is taken.
    /// The length of `values` should correspond to the number of `true`
    /// bits in `signal`.
    ///
    /// An "unlatched" spread operation is `null` when the `signal` is not
    /// `true` and the next value (`null` or otherwise) when the `signal`
    /// is `true`.
    ///
    /// A "latched" spread operation repeats the most recent signaled value for
    /// each key "as of" each output row. This is implemented by "latching" the
    /// signaled value.
    ///
    /// Parameters:
    /// * `values`: The actual values.
    /// * `grouping`: The group key for each output row. Used to determine the
    ///   "latched" value, if necessary.
    /// * `signal`: The signal indicating whether a value should be taken.
    ///
    /// `grouping` and `signal` should have the same length. The length of
    /// `values` should correspond to the number of `true` bits in `signal`.
    ///
    /// The result should have the same length as `grouping` (and `signal`).
    /// TODO: FRAZ - Error stack
    pub fn spread_signaled(
        &mut self,
        grouping: &GroupingIndices,
        values: &ArrayRef,
        signal: &BooleanArray,
    ) -> anyhow::Result<ArrayRef> {
        self.spread_impl.spread_signaled(grouping, values, signal)
    }

    /// Special case of spread when `signal` is all `true`.
    #[allow(unused)]
    pub fn spread_true(
        &mut self,
        grouping: &GroupingIndices,
        values: &ArrayRef,
    ) -> anyhow::Result<ArrayRef> {
        self.spread_impl.spread_true(grouping, values)
    }

    /// Special case of `spread` when `signal` is all `false`.
    #[allow(unused)]
    pub fn spread_false(
        &mut self,
        grouping: &GroupingIndices,
        value_type: &DataType,
    ) -> anyhow::Result<ArrayRef> {
        self.spread_impl.spread_false(grouping, value_type)
    }
}

trait SpreadImpl: Sync + Send + erased_serde::Serialize + ToSerializedSpread + std::fmt::Debug {
    /// Spread the values out according to the given signal.
    ///
    /// The result will have the same length as `signal`. For each row,
    /// if `signal` is true then the next value from `values` is taken.
    /// The length of `values` should correspond to the number of `true`
    /// bits in `signal`.
    ///
    /// An "unlatched" spread operation is `null` when the `signal` is not
    /// `true` and the next value (`null` or otherwise) when the `signal`
    /// is `true`.
    ///
    /// A "latched" spread operation repeats the most recent signaled value for
    /// each key "as of" each output row. This is implemented by "latching" the
    /// signaled value.
    ///
    /// Parameters:
    /// * `values`: The actual values.
    /// * `grouping`: The group key for each output row. Used to determine the
    ///   "latched" value, if necessary.
    /// * `signal`: The signal indicating whether a value should be taken.
    ///
    /// `grouping` and `signal` should have the same length. The length of
    /// `values` should correspond to the number of `true` bits in `signal`.
    ///
    /// The result should have the same length as `grouping` (and `signal`).
    fn spread_signaled(
        &mut self,
        grouping: &GroupingIndices,
        values: &ArrayRef,
        signal: &BooleanArray,
    ) -> anyhow::Result<ArrayRef>;

    /// Special case of spread when `signal` is all `true`.
    fn spread_true(
        &mut self,
        grouping: &GroupingIndices,
        values: &ArrayRef,
    ) -> anyhow::Result<ArrayRef>;

    /// Special case of `spread` when `signal` is all `false`.
    fn spread_false(
        &mut self,
        grouping: &GroupingIndices,
        value_type: &DataType,
    ) -> anyhow::Result<ArrayRef>;
}

// Implements `serde` for the SpreadImpl in terms of the erased serialize
// method.
erased_serde::serialize_trait_object!(SpreadImpl);

#[derive(serde::Serialize, serde::Deserialize, Debug)]
struct UnlatchedSpread;

impl ToSerializedSpread for UnlatchedSpread {
    fn to_serialized_spread(&self) -> SerializedSpread<'_> {
        SerializedSpread::Unlatched(Boo::Borrowed(self))
    }
}

impl SpreadImpl for UnlatchedSpread {
    fn spread_signaled(
        &mut self,
        grouping: &GroupingIndices,
        values: &ArrayRef,
        signal: &BooleanArray,
    ) -> anyhow::Result<ArrayRef> {
        // TODO: Grouping unsupported, but shouldn't affect results
        let mut indices = Int32Builder::with_capacity(grouping.len());
        let mut next_index = 0;
        for signal in signal.iter() {
            if signal.unwrap_or(false) {
                indices.append_value(next_index);
                next_index += 1;
            } else {
                indices.append_null();
            }
        }
        let indices = indices.finish();
        arrow::compute::take(values.as_ref(), &indices, None).context("failed to take values")
    }

    fn spread_true(
        &mut self,
        _grouping: &GroupingIndices,
        values: &ArrayRef,
    ) -> anyhow::Result<ArrayRef> {
        // TODO: Grouping unsupported:
        // anyhow::ensure!(grouping.len() == values.len());
        Ok(values.clone())
    }

    fn spread_false(
        &mut self,
        _grouping: &GroupingIndices,
        _value_type: &DataType,
    ) -> anyhow::Result<ArrayRef> {
        // Ok(new_null_array(value_type, grouping.len()))
        todo!("unimplemented - requires grouping")
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
struct LatchedSpread {
    // The index of the group is used as the index in the data.
    #[serde(with = "sparrow_arrow::serde::array_ref")]
    data: ArrayRef,
}

impl LatchedSpread {
    fn new(data_type: &DataType) -> Self {
        // TODO: Upgrade to arrow>=45 and this can be `make_empty(data_type)`
        let data = ArrayData::new_empty(data_type);
        let data = arrow::array::make_array(data);

        Self { data }
    }
}

impl ToSerializedSpread for LatchedSpread {
    fn to_serialized_spread(&self) -> SerializedSpread<'_> {
        SerializedSpread::Latched(Boo::Borrowed(self))
    }
}

impl SpreadImpl for LatchedSpread {
    fn spread_signaled(
        &mut self,
        grouping: &GroupingIndices,
        values: &ArrayRef,
        signal: &BooleanArray,
    ) -> anyhow::Result<ArrayRef> {
        debug_assert_eq!(grouping.len(), signal.len());
        anyhow::ensure!(self.data.len() <= grouping.num_groups());

        // TODO: We could do this using a separate null buffer and value buffer.
        // This would allow us to avoid copying the data from this vector to the
        // data buffers for `take`.
        let mut state_take_indices: Vec<Option<u32>> = (0..grouping.num_groups())
            .map(|index| {
                if index < self.data.len() {
                    Some(index as u32)
                } else {
                    None
                }
            })
            .collect();

        let mut indices = UInt32Builder::with_capacity(grouping.len());
        let mut next_index = self.data.len() as u32;
        for (signal, group) in signal.iter().zip(grouping.group_iter()) {
            if signal.unwrap_or(false) {
                indices.append_value(next_index);
                state_take_indices[group] = Some(next_index);
                next_index += 1;
            } else {
                indices.append_option(state_take_indices[group]);
            }
        }
        let indices = indices.finish();

        let state_take_indices = UInt32Array::from(state_take_indices);
        let result = sparrow_arrow::concat_take(&self.data, values, &indices)?;
        self.data = sparrow_arrow::concat_take(&self.data, values, &state_take_indices)?;
        Ok(result)
    }

    fn spread_true(
        &mut self,
        grouping: &GroupingIndices,
        values: &ArrayRef,
    ) -> anyhow::Result<ArrayRef> {
        anyhow::ensure!(grouping.len() == values.len());
        anyhow::ensure!(self.data.len() <= grouping.num_groups());

        // TODO: We could do this using a separate null buffer and value buffer.
        // This would allow us to avoid copying the data from this vector to the
        // data buffers for `take`.
        let mut state_take_indices: Vec<Option<u32>> = (0..grouping.num_groups())
            .map(|index| {
                if index < self.data.len() {
                    Some(index as u32)
                } else {
                    None
                }
            })
            .collect();

        // This will update the new_state_indices to the last value for each group.
        // This will null-out the data if the value is null at that point, so we
        // don't need to hadle that case specially.
        for (index, group) in grouping.group_iter().enumerate() {
            state_take_indices[group] = Some((index + self.data.len()) as u32)
        }
        let state_take_indices = UInt32Array::from(state_take_indices);
        self.data = sparrow_arrow::concat_take(&self.data, values, &state_take_indices)?;

        Ok(values.clone())
    }

    fn spread_false(
        &mut self,
        grouping: &GroupingIndices,
        _value_type: &DataType,
    ) -> anyhow::Result<ArrayRef> {
        arrow::compute::take(self.data.as_ref(), grouping.group_indices(), None)
            .context("failed to take values")
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{
        Array, ArrayRef, BooleanArray, Float64Array, Int32Builder, Int64Array, LargeStringArray,
        ListArray, MapBuilder, StringArray, StringBuilder, StructArray, UInt32Array,
    };
    use arrow::datatypes::{DataType, Field, UInt64Type};
    use sparrow_arrow::downcast::{
        downcast_primitive_array, downcast_string_array, downcast_struct_array,
    };
    use sparrow_arrow::utils::make_struct_array_null;
    use sparrow_instructions::GroupingIndices;

    use super::Spread;

    #[test]
    fn test_primitive_i64_unlatched_start_end_included() {
        let nums = Int64Array::from(vec![Some(5), Some(8), None, Some(10)]);
        let result = run_spread(
            Arc::new(nums),
            vec![0, 1, 2, 0, 1, 2, 0],
            vec![true, false, true, false, true, false, true],
            false,
        );
        let result: &Int64Array = downcast_primitive_array(result.as_ref()).unwrap();

        assert_eq!(
            result,
            &Int64Array::from(vec![Some(5), None, Some(8), None, None, None, Some(10)])
        );
    }

    #[test]
    fn test_primitive_i64_unlatched_start_end_excluded() {
        let nums = Int64Array::from(vec![Some(5), Some(8), None, Some(10)]);
        let result = run_spread(
            Arc::new(nums),
            vec![0, 1, 2, 0, 1, 2, 0, 1, 2],
            vec![false, true, false, true, false, true, false, true, false],
            false,
        );
        let result: &Int64Array = downcast_primitive_array(result.as_ref()).unwrap();

        assert_eq!(
            result,
            &Int64Array::from(vec![
                None,
                Some(5),
                None,
                Some(8),
                None,
                None,
                None,
                Some(10),
                None
            ])
        );
    }

    #[test]
    fn test_primitive_u64_latched() {
        let nums = Int64Array::from(vec![Some(5), Some(8), None, Some(10), None, Some(12)]);
        let result = run_spread(
            Arc::new(nums),
            vec![0, 1, 2, 0, 1, 2, 0, 1, 2, 0, 0],
            vec![
                false, true, false, true, false, true, false, true, false, true, false,
            ],
            true,
        );
        let result: &Int64Array = downcast_primitive_array(result.as_ref()).unwrap();

        assert_eq!(
            result,
            &Int64Array::from(vec![
                None,
                Some(5),
                None,
                Some(8),
                Some(5), // signal false, remember last value for key 1=5
                None,
                Some(8), // signal false, remember last value for key 0=8
                Some(10),
                None,
                None,
                None
            ])
        );
    }

    #[test]
    fn test_primitive_f64_latched() {
        let nums = Float64Array::from(vec![
            Some(5.0),
            Some(8.2),
            None,
            Some(10.3),
            None,
            Some(12.0),
        ]);
        let result = run_spread(
            Arc::new(nums),
            vec![0, 1, 2, 0, 1, 2, 0, 1, 2, 0, 0],
            vec![
                false, true, false, true, false, true, false, true, false, true, false,
            ],
            true,
        );
        let result: &Float64Array = downcast_primitive_array(result.as_ref()).unwrap();

        assert_eq!(
            result,
            &Float64Array::from(vec![
                None,
                Some(5.0),
                None,
                Some(8.2),
                Some(5.0), // signal false, remember last value for key 1=5
                None,
                Some(8.2), // signal false, remember last value for key 0=8
                Some(10.3),
                None,
                None,
                None
            ])
        );
    }

    #[test]
    fn test_primitive_f64_unlatched() {
        let nums = Float64Array::from(vec![
            Some(5.0),
            Some(8.2),
            None,
            Some(10.3),
            None,
            Some(12.0),
        ]);
        let result = run_spread(
            Arc::new(nums),
            vec![0, 1, 2, 0, 1, 2, 0, 1, 2, 0, 0],
            vec![
                false, true, false, true, false, true, false, true, false, true, false,
            ],
            false,
        );
        let result: &Float64Array = downcast_primitive_array(result.as_ref()).unwrap();

        assert_eq!(
            result,
            &Float64Array::from(vec![
                None,
                Some(5.0),
                None,
                Some(8.2),
                None,
                None,
                None,
                Some(10.3),
                None,
                None,
                None
            ])
        );
    }

    #[test]
    fn test_struct_unlatched() {
        let input = make_test_struct_array(vec![
            Some(TestStruct {
                a: Some(5),
                b: Some(8.7),
            }),
            None,
            Some(TestStruct {
                a: None,
                b: Some(42.3),
            }),
        ]);
        let result = run_spread(
            Arc::new(input),
            vec![0, 0, 0, 0, 0, 0],
            vec![true, false, true, false, true, false],
            false,
        );
        let result = from_test_struct_array(result);

        insta::assert_json_snapshot!(result, @r###"
        [
          {
            "a": 5,
            "b": 8.7
          },
          null,
          null,
          null,
          {
            "a": null,
            "b": 42.3
          },
          null
        ]
        "###);
    }

    #[test]
    fn test_struct_latched_same_grouping() {
        let input = make_test_struct_array(vec![
            Some(TestStruct {
                a: Some(5),
                b: Some(8.7),
            }),
            None,
            Some(TestStruct {
                a: None,
                b: Some(42.3),
            }),
        ]);
        let result = run_spread(
            Arc::new(input),
            vec![0, 0, 0, 0, 0, 0],
            vec![true, false, true, false, true, false],
            true,
        );
        let result = from_test_struct_array(result);

        insta::assert_json_snapshot!(result, @r###"
        [
          {
            "a": 5,
            "b": 8.7
          },
          {
            "a": 5,
            "b": 8.7
          },
          null,
          null,
          {
            "a": null,
            "b": 42.3
          },
          {
            "a": null,
            "b": 42.3
          }
        ]
        "###);
    }

    #[test]
    fn test_string_unlatched_start_end_included() {
        let nums = StringArray::from(vec![Some("5"), Some("8"), None, Some("10")]);
        let result = run_spread(
            Arc::new(nums),
            vec![0, 1, 2, 0, 1, 2, 0],
            vec![true, false, true, false, true, false, true],
            false,
        );
        let result: &StringArray = downcast_string_array(result.as_ref()).unwrap();

        assert_eq!(
            result,
            &StringArray::from(vec![
                Some("5"),
                None,
                Some("8"),
                None,
                None,
                None,
                Some("10")
            ])
        );
    }

    #[test]
    fn test_string_unlatched_start_end_excluded() {
        let nums = StringArray::from(vec![Some("5"), Some("8"), None, Some("10")]);
        let result = run_spread(
            Arc::new(nums),
            vec![0, 1, 2, 0, 1, 2, 0, 1, 2],
            vec![false, true, false, true, false, true, false, true, false],
            false,
        );
        let result: &StringArray = downcast_string_array(result.as_ref()).unwrap();

        assert_eq!(
            result,
            &StringArray::from(vec![
                None,
                Some("5"),
                None,
                Some("8"),
                None,
                None,
                None,
                Some("10"),
                None
            ])
        );
    }

    #[test]
    fn test_string_latched() {
        let nums = StringArray::from(vec![
            Some("5"),
            Some("8"),
            None,
            Some("10"),
            None,
            Some("12"),
        ]);
        let result = run_spread(
            Arc::new(nums),
            vec![0, 1, 2, 0, 1, 2, 0, 1, 2, 0, 0],
            vec![
                false, true, false, true, false, true, false, true, false, true, false,
            ],
            true,
        );
        let result: &StringArray = downcast_string_array(result.as_ref()).unwrap();

        assert_eq!(
            result,
            &StringArray::from(vec![
                None,
                Some("5"),
                None,
                Some("8"),
                Some("5"), // signal false, remember last value for key 1=5
                None,
                Some("8"), // signal false, remember last value for key 0=8
                Some("10"),
                None,
                None,
                None
            ])
        );
    }

    #[test]
    fn test_large_string_latched() {
        let nums = LargeStringArray::from(vec![
            Some("5"),
            Some("8"),
            None,
            Some("10"),
            None,
            Some("12"),
        ]);
        let result = run_spread(
            Arc::new(nums),
            vec![0, 1, 2, 0, 1, 2, 0, 1, 2, 0, 0],
            vec![
                false, true, false, true, false, true, false, true, false, true, false,
            ],
            true,
        );
        let result: &LargeStringArray = downcast_string_array(result.as_ref()).unwrap();

        assert_eq!(
            result,
            &LargeStringArray::from(vec![
                None,
                Some("5"),
                None,
                Some("8"),
                Some("5"), // signal false, remember last value for key 1=5
                None,
                Some("8"), // signal false, remember last value for key 0=8
                Some("10"),
                None,
                None,
                None
            ])
        );
    }

    #[test]
    fn test_large_string_unlatched() {
        let nums = LargeStringArray::from(vec![
            Some("5"),
            Some("8"),
            None,
            Some("10"),
            None,
            Some("12"),
        ]);
        let result = run_spread(
            Arc::new(nums),
            vec![0, 1, 2, 0, 1, 2, 0, 1, 2, 0, 0],
            vec![
                false, true, false, true, false, true, false, true, false, true, false,
            ],
            false,
        );
        let result: &LargeStringArray = downcast_string_array(result.as_ref()).unwrap();

        assert_eq!(
            result,
            &LargeStringArray::from(vec![
                None,
                Some("5"),
                None,
                Some("8"),
                None,
                None,
                None,
                Some("10"),
                None,
                None,
                None
            ])
        );
    }

    #[test]
    fn test_unlatched_uint64_list_spread() {
        let data = vec![
            Some(vec![]),
            None,
            Some(vec![Some(3), Some(5), Some(19)]),
            Some(vec![Some(6)]),
        ];
        let list_array = ListArray::from_iter_primitive::<UInt64Type, _, _>(data);

        let result = run_spread(
            Arc::new(list_array),
            vec![0, 1, 2, 3, 4, 5, 6, 7],
            vec![true, false, false, true, false, true, false, true],
            false,
        );

        let expected = vec![
            Some(vec![]),
            None,
            None,
            None,
            None,
            Some(vec![Some(3), Some(5), Some(19)]),
            None,
            Some(vec![Some(6)]),
        ];
        let expected = ListArray::from_iter_primitive::<UInt64Type, _, _>(expected);

        let expected: ArrayRef = Arc::new(expected);
        assert_eq!(&result, &expected)
    }

    #[test]
    fn test_latched_uint64_list_spread() {
        let data = vec![
            Some(vec![]),
            None,
            Some(vec![Some(3), Some(5), Some(19)]),
            Some(vec![Some(6)]),
        ];
        let list_array = ListArray::from_iter_primitive::<UInt64Type, _, _>(data);

        let result = run_spread(
            Arc::new(list_array),
            vec![0, 1, 0, 0, 0, 0, 0, 0],
            vec![true, false, false, true, false, true, false, true],
            true,
        );

        let expected = vec![
            Some(vec![]),
            None,
            Some(vec![]),
            None,
            None,
            Some(vec![Some(3), Some(5), Some(19)]),
            Some(vec![Some(3), Some(5), Some(19)]),
            Some(vec![Some(6)]),
        ];
        let expected = ListArray::from_iter_primitive::<UInt64Type, _, _>(expected);

        let expected: ArrayRef = Arc::new(expected);
        assert_eq!(&result, &expected)
    }

    #[test]
    fn test_unlatched_uint64_list_spread_sliced() {
        let data = vec![
            Some(vec![Some(1)]),
            Some(vec![]),
            None,
            Some(vec![Some(3), Some(5), Some(19)]),
            Some(vec![Some(6)]),
        ];
        let list_array = ListArray::from_iter_primitive::<UInt64Type, _, _>(data);
        let list_array = list_array.slice(1, 4);
        let list_array = Arc::new(list_array);

        let result = run_spread(
            list_array,
            vec![0, 1, 2, 3, 4, 5, 6, 7],
            vec![true, false, false, true, false, true, false, true],
            false,
        );

        let expected = vec![
            Some(vec![]),
            None,
            None,
            None,
            None,
            Some(vec![Some(3), Some(5), Some(19)]),
            None,
            Some(vec![Some(6)]),
        ];
        let expected = ListArray::from_iter_primitive::<UInt64Type, _, _>(expected);

        let expected: ArrayRef = Arc::new(expected);
        assert_eq!(&result, &expected)
    }

    #[test]
    fn test_unlatched_map_spread() {
        let string_builder = StringBuilder::new();
        let int_builder = Int32Builder::with_capacity(8);
        let mut builder = MapBuilder::new(None, string_builder, int_builder);

        builder.keys().append_value("joe");
        builder.values().append_value(1);
        builder.append(true).unwrap();

        builder.keys().append_value("blogs");
        builder.values().append_value(2);
        builder.keys().append_value("foo");
        builder.values().append_value(4);
        builder.append(true).unwrap();

        builder.append(false).unwrap();

        builder.keys().append_value("joe");
        builder.values().append_value(10);
        builder.keys().append_value("foo");
        builder.values().append_value(1);
        builder.append(true).unwrap();

        builder.keys().append_value("alice");
        builder.values().append_value(2);
        builder.append(true).unwrap();

        let map_array = builder.finish();

        let result = run_spread(
            Arc::new(map_array),
            vec![0, 1, 2, 3, 4, 5, 6, 7],
            vec![true, false, false, true, false, true, false, true],
            false,
        );

        let string_builder2 = StringBuilder::new();
        let int_builder2 = Int32Builder::with_capacity(8);
        let mut builder = MapBuilder::new(None, string_builder2, int_builder2);
        builder.keys().append_value("joe");
        builder.values().append_value(1);
        builder.append(true).unwrap();

        builder.append(false).unwrap();
        builder.append(false).unwrap();

        builder.keys().append_value("blogs");
        builder.values().append_value(2);
        builder.keys().append_value("foo");
        builder.values().append_value(4);
        builder.append(true).unwrap();

        builder.append(false).unwrap();
        builder.append(false).unwrap();
        builder.append(false).unwrap();

        builder.keys().append_value("joe");
        builder.values().append_value(10);
        builder.keys().append_value("foo");
        builder.values().append_value(1);
        builder.append(true).unwrap();

        let expected = builder.finish();
        let expected: ArrayRef = Arc::new(expected);
        assert_eq!(&result, &expected)
    }

    fn run_spread(
        values: ArrayRef,
        indices: Vec<u32>,
        signal: Vec<bool>,
        latched: bool,
    ) -> ArrayRef {
        debug_assert_eq!(indices.len(), signal.len());
        let mut spread = Spread::try_new(latched, values.data_type()).unwrap();
        let num_groups = indices.iter().copied().max().unwrap() + 1;
        let grouping = GroupingIndices::new(num_groups as usize, UInt32Array::from(indices));

        let signal = BooleanArray::from(signal);
        let result = spread.spread_signaled(&grouping, &values, &signal).unwrap();

        assert_round_trips(&spread);

        result
    }
    #[derive(serde::Serialize)]
    struct TestStruct {
        a: Option<i64>,
        b: Option<f64>,
    }

    fn make_test_struct_array(values: Vec<Option<TestStruct>>) -> StructArray {
        let a: Int64Array = values
            .iter()
            .map(|v| v.as_ref().and_then(|v| v.a))
            .collect();
        let b: Float64Array = values
            .iter()
            .map(|v| v.as_ref().and_then(|v| v.b))
            .collect();

        let null_buffer = values.iter().map(|v| v.is_some()).collect();
        make_struct_array_null(
            values.len(),
            vec![
                (
                    // TODO(https://github.com/kaskada-ai/kaskada/issues/417): Avoid copying.
                    Arc::new(Field::new("a", DataType::Int64, true)),
                    Arc::new(a),
                ),
                (
                    // TODO(https://github.com/kaskada-ai/kaskada/issues/417): Avoid copying.
                    Arc::new(Field::new("b", DataType::Float64, true)),
                    Arc::new(b),
                ),
            ],
            null_buffer,
        )
    }

    fn from_test_struct_array(array: ArrayRef) -> Vec<Option<TestStruct>> {
        let struct_array = downcast_struct_array(array.as_ref()).unwrap();
        let a: &Int64Array = downcast_primitive_array(struct_array.column(0).as_ref()).unwrap();
        let b: &Float64Array = downcast_primitive_array(struct_array.column(1).as_ref()).unwrap();

        a.iter()
            .zip(b.iter())
            .enumerate()
            .map(|(index, (a, b))| {
                if struct_array.is_null(index) {
                    None
                } else {
                    Some(TestStruct { a, b })
                }
            })
            .collect()
    }

    fn assert_round_trips(spread: &Spread) {
        let serialized = postcard::to_stdvec(spread).unwrap();
        let deserialized: Spread = postcard::from_bytes(&serialized).unwrap();
        let reserialized = postcard::to_stdvec(&deserialized).unwrap();
        assert_eq!(serialized, reserialized)
    }

    #[test]
    fn test_bool_latched_serde() {
        let spread = Spread::try_new(true, &DataType::Boolean).unwrap();
        assert_round_trips(&spread);
    }

    #[test]
    fn test_i64_latched_serde() {
        let spread = Spread::try_new(true, &DataType::Int64).unwrap();
        assert_round_trips(&spread);
    }

    #[test]
    fn test_i64_unlatched_serde() {
        let spread = Spread::try_new(false, &DataType::Int64).unwrap();
        assert_round_trips(&spread);
    }

    #[test]
    fn test_i16_latched_serde() {
        let spread = Spread::try_new(true, &DataType::Int16).unwrap();
        assert_round_trips(&spread);
    }

    #[test]
    fn test_i16_unlatched_serde() {
        let spread = Spread::try_new(false, &DataType::Int16).unwrap();
        assert_round_trips(&spread);
    }

    #[test]
    fn test_u16_latched_serde() {
        let spread = Spread::try_new(true, &DataType::UInt16).unwrap();
        assert_round_trips(&spread);
    }

    #[test]
    fn test_u16_unlatched_serde() {
        let spread = Spread::try_new(false, &DataType::UInt16).unwrap();
        assert_round_trips(&spread);
    }
}
