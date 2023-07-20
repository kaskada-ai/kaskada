use std::sync::Arc;

use anyhow::Context;
use arrow::array::ArrayRef;
use arrow::datatypes::DataType;
use sparrow_plan::ValueRef;
use sparrow_arrow::scalar_value::ScalarValue;
use pyo3::prelude::*;
use pyo3::types::IntoPyDict;
use arrow::array::{Array, ArrayData};
use arrow::array;
use arrow::pyarrow::ToPyArrow;
use arrow::pyarrow::FromPyArrow;
use anyhow::ensure;

use crate::{Evaluator, EvaluatorFactory, RuntimeInfo, StaticInfo, ColumnarValue};

/// Evaluator for the `is_valid` instruction.
pub(super) struct IsValidEvaluator {
    input: ValueRef,
}

impl Evaluator for IsValidEvaluator {
    fn evaluate(&mut self, info: &dyn RuntimeInfo) -> anyhow::Result<ArrayRef> {
        let input = info.value(&self.input)?.array_ref()?;
        let result = arrow::compute::is_not_null(input.as_ref())?;
        Ok(Arc::new(result))
    }
}

impl EvaluatorFactory for IsValidEvaluator {
    fn try_new(info: StaticInfo<'_>) -> anyhow::Result<Box<dyn Evaluator>> {
        let input = info.unpack_argument()?;
        Ok(Box::new(Self { input }))
    }
}

/// Evaluator for the `hash` instruction.
pub(super) struct HashEvaluator {
    input: ValueRef,
}

impl Evaluator for HashEvaluator {
    fn evaluate(&mut self, info: &dyn RuntimeInfo) -> anyhow::Result<ArrayRef> {
        let input = info.value(&self.input)?.array_ref()?;
        let result = sparrow_arrow::hash::hash(input.as_ref())?;
        Ok(Arc::new(result))
    }
}

impl EvaluatorFactory for HashEvaluator {
    fn try_new(info: StaticInfo<'_>) -> anyhow::Result<Box<dyn Evaluator>> {
        let input = info.unpack_argument()?;
        Ok(Box::new(Self { input }))
    }
}

/// Evaluator for the `coalesce` instruction.
pub(super) struct CoalesceEvaluator {
    values: Vec<ValueRef>,
}

impl EvaluatorFactory for CoalesceEvaluator {
    fn try_new(info: StaticInfo<'_>) -> anyhow::Result<Box<dyn Evaluator>> {
        let values = info.args.iter().map(|arg| arg.value_ref.clone()).collect();
        Ok(Box::new(Self { values }))
    }
}

impl Evaluator for CoalesceEvaluator {
    fn evaluate(&mut self, info: &dyn RuntimeInfo) -> anyhow::Result<ArrayRef> {
        self.values
            .iter()
            .map(|value_ref| info.value(value_ref)?.array_ref())
            .reduce(|accum, item| {
                // TODO: When we switch to `arrow2` use the `if_then_else` kernel, or create
                // a "multi-way" version of that.

                let accum = accum?;
                let item = item?;

                let use_item = arrow::compute::is_null(accum.as_ref())?;
                Ok(arrow::compute::kernels::zip::zip(
                    &use_item,
                    item.as_ref(),
                    accum.as_ref(),
                )?)
            })
            .context("no values for coalesce")?
    }
}

/// Evaluator for the `python_udf` instruction.
pub(super) struct PythonUDFEvaluator {
    values: Vec<ValueRef>,
    result_type: DataType,
    code: String,
}

impl EvaluatorFactory for PythonUDFEvaluator {
    fn try_new(info: StaticInfo<'_>) -> anyhow::Result<Box<dyn Evaluator>> {
        // TODO: Parse args more carefully - the first arg is currently the class name
        let (first, rest) = info.args.split_first().context("Expected args to be non-empty")?;

        let code =
            if let Some(ScalarValue::Utf8(Some(code))) = first.value_ref.literal_value() {
                code.clone()
            } else {
                anyhow::bail!(
                    "Unable to create Python UDF with unexpected code {:?}",
                    first
                );
            };

        let values = rest.iter().map(|arg| arg.value_ref.clone()).collect();
        let result_type = info.result_type.clone();

        Ok(Box::new(Self { values, result_type, code }))
    }
}

impl Evaluator for PythonUDFEvaluator {
    fn evaluate(&mut self, info: &dyn RuntimeInfo) -> anyhow::Result<ArrayRef> {
        Python::with_gil(|py| -> anyhow::Result<ArrayRef> {
            let args: Vec<PyObject> = self.values.iter().map::<anyhow::Result<_>,_>(|value_ref| {
                let value: ColumnarValue = info.value(value_ref)?; 
                let array_ref: ArrayRef = value.array_ref()?; 
                let array_data: ArrayData = array_ref.to_data();
                let py_obj: PyObject = array_data.to_pyarrow(py)?;

                Ok(py_obj)
            }).collect::<anyhow::Result<_>>()?;

            let locals = [("args", args)].into_py_dict(py);
            py.run(&self.code, None, Some(&locals))?;
            let result_py: PyObject = locals.get_item("ret").unwrap().into();

            let result_array_data: ArrayData = ArrayData::from_pyarrow(result_py.as_ref(py))?;

            // We can't control the returned type, but we can refuse to accept the "wrong" type.
            ensure!(*result_array_data.data_type() == self.result_type, "expected Python UDF to return type {:?}, but received {:?}", result_array_data.data_type(), self.result_type);

            let result_array_ref: ArrayRef = array::make_array(result_array_data);

            // This is a "map" operation - each row should reflect the time of the 
            // corresponding input row, and have the same length.
            ensure!(result_array_ref.len() == info.num_rows(), "unexpected result length from Python UDF");

            Ok(result_array_ref)
        })
    }
}
