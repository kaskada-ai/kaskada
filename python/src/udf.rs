use itertools::Itertools;
use std::fmt::Debug;
use std::sync::Arc;

use arrow::array::{ArrayData, ArrayRef};
use arrow::datatypes::DataType;
use arrow::pyarrow::{FromPyArrow, ToPyArrow};
use pyo3::prelude::*;
use pyo3::types::PyTuple;
use sparrow_instructions::{Evaluator, RuntimeInfo, StaticInfo, ValueRef};
use sparrow_syntax::{FeatureSetPart, Signature};
use uuid::Uuid;

#[derive(Debug)]
struct PyUdf {
    signature: Signature,
    callable: Py<PyAny>,
    uuid: Uuid,
}

struct PyUdfEvaluator {
    args: Vec<ValueRef>,
    callable: Py<PyAny>,
    result_type: DataType,
}

#[derive(Clone)]
#[pyclass]
pub(crate) struct Udf(pub Arc<dyn sparrow_instructions::Udf>);

impl sparrow_instructions::Udf for PyUdf {
    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn make_evaluator(&self, info: StaticInfo<'_>) -> Box<dyn Evaluator> {
        // Idea: Create a random value of each argument type and verify that the callbale
        // is invokable on that. This would let us detect errors early.
        //
        // Though, it still wouldn't give us compile-time errors.

        let result_type = info.result_type.clone();
        let args = info.args.iter().map(|arg| arg.value_ref.clone()).collect();

        Box::new(PyUdfEvaluator {
            args,
            callable: self.callable.clone(),
            result_type,
        })
    }

    fn uuid(&self) -> &uuid::Uuid {
        &self.uuid
    }
}

impl Evaluator for PyUdfEvaluator {
    fn evaluate(&mut self, info: &dyn RuntimeInfo) -> anyhow::Result<ArrayRef> {
        let inputs = self
            .args
            .iter()
            .map(|value_ref| -> anyhow::Result<_> { info.value(value_ref)?.array_ref() })
            .try_collect()?;

        let result =
            Python::with_gil(|py| -> anyhow::Result<ArrayRef> { self.evaluate_py(py, inputs) })?;

        anyhow::ensure!(
            result.len() == info.num_rows(),
            "result has wrong number of rows"
        );
        Ok(result)
    }
}

impl PyUdfEvaluator {
    fn evaluate_py(&self, py: Python<'_>, inputs: Vec<ArrayRef>) -> anyhow::Result<ArrayRef> {
        // Convert all arg inputs into pyarrow arrays then to python objects
        let inputs: Vec<PyObject> = inputs
            .iter()
            .map::<anyhow::Result<_>, _>(|arg_array| {
                let array_data: ArrayData = arg_array.to_data();
                let py_obj: PyObject = array_data.to_pyarrow(py)?;
                Ok(py_obj)
            })
            .collect::<anyhow::Result<_>>()?;

        // The callable expects [result_type, *args]
        let mut py_inputs = vec![DataType::to_pyarrow(&self.result_type, py)?];
        py_inputs.extend(inputs);

        let inputs = PyTuple::new(py, py_inputs);

        // Execute the python udf
        let py_result = self.callable.call1(py, inputs)?;

        let result_array_data: ArrayData = ArrayData::from_pyarrow(py_result.as_ref(py))?;

        // We attempt to coerce the return type into our expected result type, but this
        // check exists to ensure the roundtrip type is as expected.
        anyhow::ensure!(
            *result_array_data.data_type() == self.result_type,
            "expected Python UDF to return type {:?}, but received {:?}",
            self.result_type,
            result_array_data.data_type(),
        );

        let result: ArrayRef = arrow::array::make_array(result_array_data);
        Ok(result)
    }
}

#[pymethods]
impl Udf {
    #[new]
    #[pyo3(signature = (signature, callable))]
    fn new(signature: String, callable: Py<PyAny>) -> Self {
        let uuid = Uuid::new_v4();
        let signature = Signature::try_from_str(FeatureSetPart::Function("udf"), &signature)
            .expect("signature to parse");
        let udf = PyUdf {
            signature,
            callable,
            uuid,
        };
        Udf(Arc::new(udf))
    }
}
