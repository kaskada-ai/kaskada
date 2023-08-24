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

        // 2. Call the python function
        let result_ty = DataType::to_pyarrow(&self.result_type, py)?;
        let result_ty_vec: Vec<PyObject> = vec![result_ty];
        let inputs: Vec<_> = result_ty_vec.into_iter().chain(inputs).collect();
        let inputs = PyTuple::new(py, inputs);

        println!("CALLING CALLABLE ");
        let py_result = self.callable.call1(py, inputs)?;

        // let kwargs = PyDict::new(py);
        // // Set the 'result_type' key to a specific value
        // let result_ty = DataType::to_pyarrow(&self.result_type, py)?;
        // kwargs.set_item("result_type", result_ty)?;

        // let inputs = PyTuple::new(py, inputs);
        // let py_result = self.callable.call(py, inputs, Some(kwargs))?;

        //////////
        // let mut new_args: Vec<PyObject> = vec![DataType::to_pyarrow(&self.result_type, py)?];

        // // Collect the other arguments
        // for input in inputs.iter() {
        //     let array_data: ArrayData = input.to_data();
        //     let py_obj: PyObject = array_data.to_pyarrow(py)?;
        //     new_args.push(py_obj);
        // }

        // println!("New args: {:?}", new_args);
        // let args = PyTuple::new(py, new_args);
        // let py_result = self.callable.call(py, args, Some(kwargs))?;

        // 3. Convert the result from pyarrow array to arrow array
        let result_array_data: ArrayData = ArrayData::from_pyarrow(py_result.as_ref(py))?;

        // We can't control the returned type, but we can refuse to accept the "wrong" type.
        anyhow::ensure!(
            *result_array_data.data_type() == self.result_type,
            "expected Python UDF to return type {:?}, but received {:?}",
            result_array_data.data_type(),
            self.result_type
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
        println!("Creating udf with signature: {:?}", signature);
        let uuid = Uuid::new_v4();
        // TODO: sig name string cannot be &'static str
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
