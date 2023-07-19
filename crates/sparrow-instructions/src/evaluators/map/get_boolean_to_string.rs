use std::marker::PhantomData;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, BooleanArray, GenericStringArray, OffsetSizeTrait};
use arrow::datatypes::DataType;
use itertools::Itertools;
use sparrow_arrow::downcast::{downcast_boolean_array, downcast_string_array};

use sparrow_plan::ValueRef;

use crate::{Evaluator, EvaluatorFactory, StaticInfo};

/// Evaluator for `get` on maps for boolean keys and string values.
#[derive(Debug)]
pub(in crate::evaluators) struct GetBooleanToStringEvaluator<O>
where
    O: OffsetSizeTrait,
{
    map: ValueRef,
    key: ValueRef,
    // Make the compiler happy by using the type parameters
    _phantom: PhantomData<O>,
}

impl<O> EvaluatorFactory for GetBooleanToStringEvaluator<O>
where
    O: OffsetSizeTrait,
{
    fn try_new(info: StaticInfo<'_>) -> anyhow::Result<Box<dyn Evaluator>> {
        let key_type = info.args[0].data_type.clone();
        anyhow::ensure!(
            matches!(key_type, DataType::Boolean),
            "expected boolean key type, saw {:?}",
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

        anyhow::ensure!(
            matches!(value_type, DataType::Utf8 | DataType::LargeUtf8),
            "expected string value type, saw {:?}",
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

impl<O> Evaluator for GetBooleanToStringEvaluator<O>
where
    O: OffsetSizeTrait,
{
    fn evaluate(&mut self, info: &dyn crate::RuntimeInfo) -> anyhow::Result<ArrayRef> {
        let map_input = info.value(&self.map)?.map_array()?;
        let key_input = info.value(&self.key)?.boolean_array()?;

        let result: GenericStringArray<O> = {
            anyhow::ensure!(
                key_input.len() == map_input.len(),
                "key and map lengths don't match"
            );
            let values: Vec<Option<String>> = (0..key_input.len())
                .map(|i| -> anyhow::Result<_> {
                    let cur_key = key_input.value(i);
                    let cur_map = map_input.value(i);
                    debug_assert!(cur_map.fields().len() == 2);

                    // Iterate through map_entry, match on key, return value.
                    // Note: if the map were ordered, we could more efficiently find the value.
                    let m_keys: &BooleanArray = downcast_boolean_array(cur_map.column(0).as_ref())?;
                    let m_values: &GenericStringArray<O> =
                        downcast_string_array(cur_map.column(1).as_ref())?;

                    let value_pos = m_keys.iter().position(|x| x == Some(cur_key));
                    match value_pos {
                        Some(value_pos) => {
                            if m_values.is_null(value_pos) {
                                Ok(None)
                            } else {
                                Ok(Some(m_values.value(value_pos).to_owned()))
                            }
                        }
                        None => Ok(None),
                    }
                })
                .try_collect()?;

            GenericStringArray::<O>::from_iter(values.iter())
        };

        Ok(Arc::new(result))
    }
}
