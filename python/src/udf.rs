use itertools::Itertools;
use std::fmt::Debug;
use std::sync::Arc;

use arrow::array::{new_empty_array, ArrayData, ArrayRef};
use arrow::datatypes::DataType;
use arrow::pyarrow::{FromPyArrow, ToPyArrow};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyTuple};
use sparrow_instructions::{ColumnarValue, Evaluator, RuntimeInfo, StaticInfo, ValueRef};
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
// TODO: Does it need to extend Expr for some reason?
pub(crate) struct Udf(pub Arc<dyn sparrow_instructions::Udf>);

impl sparrow_instructions::Udf for PyUdf {
    fn signature(&self) -> &Signature {
        &self.signature
    }

    // FRAZ: Should we allow this to return an error?
    fn make_evaluator(&self, info: StaticInfo<'_>) -> Box<dyn Evaluator> {
        // TODO: Idea: Create a random value of each argument type and verify that the callbale
        // is invokable on that. This would let us detect errors early.

        println!("Making python udf evaluator");
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
        println!("Evaluating Python UDF");
        let inputs = self
            .args
            .iter()
            .map(|value_ref| -> anyhow::Result<_> { Ok(info.value(value_ref)?.array_ref()?) })
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
    fn evaluate_py<'py>(&self, py: Python<'py>, inputs: Vec<ArrayRef>) -> anyhow::Result<ArrayRef> {
        // 1. Convert all arg inputs into pyarrow arrays then to python objects
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

        // let inputs: Vec<_> = result_ty_vec.into_iter().chain(inputs).collect();
        let inputs = PyTuple::new(py, py_inputs);

        // 3. Execute the udf
        let py_result = self.callable.call1(py, inputs)?;

        // 3. Convert the result from pyarrow array to arrow array
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
