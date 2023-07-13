use std::marker::PhantomData;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, LargeStringArray, PrimitiveArray, StringArray};
use arrow::datatypes::{ArrowPrimitiveType, DataType};
use itertools::Itertools;
use owning_ref::ArcRef;
use sparrow_arrow::downcast::{downcast_primitive_array, downcast_string_array};

use sparrow_plan::ValueRef;

use crate::{Evaluator, EvaluatorFactory, StaticInfo};

/// Evaluator for `get` on maps for large string (LargeUtf8) keys and primitive values.
#[derive(Debug)]
pub(in crate::evaluators) struct GetLargeStringToPrimitiveEvaluator<T>
where
    T: ArrowPrimitiveType + Sync + Send,
{
    map: ValueRef,
    key: ValueRef,
    // Make the compiler happy by using the type parameter
    _phantom: PhantomData<T>,
}

impl<T> EvaluatorFactory for GetLargeStringToPrimitiveEvaluator<T>
where
    T: ArrowPrimitiveType + Sync + Send,
{
    fn try_new(info: StaticInfo<'_>) -> anyhow::Result<Box<dyn Evaluator>> {
        let key_type = info.args[0].data_type.clone();

        // Key should be a string
        anyhow::ensure!(
            matches!(key_type, DataType::LargeUtf8),
            "expected large_string key type, saw {:?}",
            key_type
        );

        let map_type = &info.args[1].data_type;
        let value_type = match map_type {
            DataType::Map(s, _) => match s.data_type() {
                DataType::Struct(fields) => {
                    anyhow::ensure!(
                        fields.len() == 2,
                        "expected 2 fields in map, saw {:?}",
                        fields
                    );
                    anyhow::ensure!(
                        fields[0].data_type() == &key_type,
                        "expected key type {:?}, saw {:?}",
                        key_type,
                        map_type
                    );
                    fields[1].data_type().clone()
                }
                other => anyhow::bail!("expected struct type in map, saw {:?}", other),
            },
            other => anyhow::bail!("expected map type, saw {:?}", other),
        };

        // Value should be a primitive type
        anyhow::ensure!(
            value_type.is_primitive(),
            "expected primitive value type in map, saw {:?}",
            value_type
        );

        let (key, map) = info.unpack_arguments()?;
        Ok(Box::new(Self {
            map,
            key,
            _phantom: PhantomData,
        }))
    }
}

impl<T> Evaluator for GetLargeStringToPrimitiveEvaluator<T>
where
    T: ArrowPrimitiveType + Sync + Send,
{
    fn evaluate(&mut self, info: &dyn crate::RuntimeInfo) -> anyhow::Result<ArrayRef> {
        let map_input = info.value(&self.map)?.map_array()?;
        let key_input = info.value(&self.key)?.string_array::<i64>()?;

        let result: PrimitiveArray<T> = {
            anyhow::ensure!(
                key_input.len() == map_input.len(),
                "key and map lengths don't match"
            );
            let values: Vec<Option<T::Native>> = (0..key_input.len())
                .map(|i| -> anyhow::Result<_> {
                    let cur_key = key_input.value(i);
                    let cur_map = map_input.value(i);
                    debug_assert!(cur_map.fields().len() == 2);

                    // Iterate through map_entry, match on key, return value.
                    // Note: if the map were ordered, we could more efficiently find the value.
                    let m_keys: &LargeStringArray =
                        downcast_string_array(cur_map.column(0).as_ref())?;
                    let m_values: &PrimitiveArray<T> =
                        downcast_primitive_array(cur_map.column(1).as_ref())?;

                    let value_pos = m_keys.iter().position(|x| x == Some(cur_key));
                    match value_pos {
                        Some(value_pos) => {
                            if m_values.is_null(value_pos) {
                                Ok(None)
                            } else {
                                Ok(Some(m_values.value(value_pos)))
                            }
                        }
                        None => Ok(None),
                    }
                })
                .try_collect()?;

            // SAFETY: `map` is a trusted length iterator
            unsafe { PrimitiveArray::from_trusted_len_iter(values.iter()) }
        };

        Ok(Arc::new(result))
    }
}
