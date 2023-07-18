use std::sync::Arc;

use anyhow::Context;
use arrow::array::ArrayRef;
use sparrow_plan::ValueRef;
use pyo3::prelude::*;
use pyo3::types::IntoPyDict;
use arrow::datatypes::DataType;
use arrow::array::Array;
use arrow::array;

use crate::{Evaluator, EvaluatorFactory, RuntimeInfo, StaticInfo};

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
}

impl EvaluatorFactory for PythonUDFEvaluator {
    fn try_new(info: StaticInfo<'_>) -> anyhow::Result<Box<dyn Evaluator>> {
        let values = info.args.iter().map(|arg| arg.value_ref.clone()).collect();
        Ok(Box::new(Self { values }))
    }
}

impl Evaluator for PythonUDFEvaluator {
    fn evaluate(&mut self, info: &dyn RuntimeInfo) -> anyhow::Result<ArrayRef> {
        // let chunked_arrays: Vec<_> = self.values.iter().map(|value_ref| {
        //     let chunk = info.value(value_ref)?.array_ref()?;
        //     ChunkedArray::from_chunks("foo", vec!(chunk)) // <- ISSUE: This is an arrow array, but needs to be an arrow2 array for polars
        // }).collect();

        let array_refs: anyhow::Result<Vec<_>> = self.values.iter().map::<anyhow::Result<_>,_>(|value_ref| Ok(info.value(value_ref)?.array_ref()?)).collect();

        Python::with_gil(|py| -> PyResult<()> {
            for i in 0..info.num_rows() {
                let values: Vec<Py<PyAny>> = array_refs.iter().map(|array_ref| match array_ref.data_type() {
                    DataType::Null => array::as_null_array(array_ref).value(i).into(),
                    // DataType::Boolean => Array::as_boolean_array(array_ref).value(i).into(),
                    // DataType::Int32 => Array::as_primitive_array::<Int32Type>(array_ref).value(i).into(),
                    // DataType::Int64 => Array::as_primitive_array::<Int64Type>(array_ref).value(i).into(),
                    // DataType::Float32 => Array::as_primitive_array::<Float32Type>(array_ref).value(i).into(),
                    // DataType::Float64 => Array::as_primitive_array::<Float64Type>(array_ref).value(i).into(),
                    // DataType::Struct(_) => Array::as_struct_array(array_ref),
                });
            }
            // let code = printf("udf = new {}\n udf.map(input)", "module.Class");
            // let locals = [("input", PySeries(chunked_arrays[0].into_series()))].into_py_dict(py);
            // let result = py.eval(code, None, Some(&locals))?.extract()?;

            Ok(())
        });
        
        // let primitive_array = array_refs[0].as_primitive_opt::<i64>()?
        // let value = primitive_array.value(0);

        self.values
            .iter()
            .map(|value_ref| info.value(value_ref)?.array_ref())
            .reduce(|accum, item| {
                // TODO: When we switch to `arrow2` use the `if_then_else` kernel, or create
                // a "multi-way" version of that.

                Python::with_gil(|py| -> PyResult<()> {
                    let sys = py.import("sys")?;
                    let version: String = sys.getattr("version")?.extract()?;
            
                    let locals = [("os", py.import("os")?)].into_py_dict(py);
                    let code = "os.getenv('USER') or os.getenv('USERNAME') or 'Unknown'";
                    let user: String = py.eval(code, None, Some(&locals))?.extract()?;
            
                    println!("Hello {}, I'm Python {}", user, version);
                    Ok(())
                });

                let accum = accum?;
                let item = item?;

                let use_item = arrow::compute::is_null(accum.as_ref())?;
                Ok(arrow::compute::kernels::zip::zip(
                    &use_item,
                    item.as_ref(),
                    accum.as_ref(),
                )?)
            })
            .context("no values for python_udf")?
    }
}
