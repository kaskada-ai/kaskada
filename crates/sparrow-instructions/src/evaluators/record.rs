//! The record instruction isn't a "normal" instruction since it takes
//! a variable number of arguments and doesn't have a fixed input/output
//! type.

use std::sync::Arc;

use crate::ValueRef;
use anyhow::anyhow;
use arrow::array::ArrayRef;
use arrow::datatypes::{DataType, FieldRef, Fields};
use itertools::Itertools;
use sparrow_arrow::utils::make_struct_array;

use crate::{Evaluator, EvaluatorFactory, RuntimeInfo, StaticInfo};

#[derive(Debug)]
pub struct RecordEvaluator {
    args: Vec<ValueRef>,
    fields: Fields,
}

impl Evaluator for RecordEvaluator {
    fn evaluate(&mut self, info: &dyn RuntimeInfo) -> anyhow::Result<ArrayRef> {
        let fields: Vec<(FieldRef, ArrayRef)> = self
            .fields
            .iter()
            .zip(self.args.iter())
            .map(|(field, arg)| info.value(arg)?.array_ref().map(|i| (field.clone(), i)))
            .try_collect()?;

        let result = make_struct_array(info.num_rows(), fields);
        Ok(Arc::new(result))
    }
}

impl EvaluatorFactory for RecordEvaluator {
    fn try_new(info: StaticInfo<'_>) -> anyhow::Result<Box<dyn Evaluator>> {
        let args = info.args;
        let result_type = info.result_type;
        if let DataType::Struct(fields) = &result_type {
            anyhow::ensure!(
                args.len() == fields.len() * 2,
                "Mismatched arguments for RecordEvaluator. Expected {:?} but was {:?}",
                fields.len() * 2,
                args.len()
            );

            let args = args
                .into_iter()
                .skip(1)
                .step_by(2)
                .zip(fields)
                .map(|(arg, field)| {
                    anyhow::ensure!(
                        &arg.data_type == field.data_type(),
                        "Expected argument type ({:?}) to match field type ({:?}",
                        &arg.data_type,
                        field.data_type()
                    );
                    Ok(arg.value_ref)
                })
                .try_collect()?;
            Ok(Box::new(Self {
                args,
                fields: fields.clone(),
            }))
        } else {
            Err(anyhow!(
                "Record instruction can't produce {:?}",
                &result_type
            ))
        }
    }
}
