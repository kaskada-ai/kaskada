use arrow::array::ArrayData;
use arrow::datatypes::DataType;
use arrow::pyarrow::{FromPyArrow, ToPyArrow};
use pyo3::prelude::*;
use pyo3::types::PyTuple;

struct PyUdf {
    signature: Signature,
    callable: Py<PyAny>,
    uuid: uuid::Uuid,
}

struct PyUdfEvaluator {
    args: Vec<ValueRef>,
    callable: Py<PyAny>,
}

#[derive(Clone)]
#[pyclass]
pub(crate) struct Udf(Arc<dyn sparrow_session::Udf>);

impl sparrow_session::Udf for PyUdf {
    fn signature(&self) -> &Signature {
        &self.signatrue
    }

    // FRAZ: Should we allow this to return an error?
    fn make_evaluator(&self, static_info: StaticInfo<'_>) -> Box<dyn Evaluator> {
        // Idea: Create a random value of each argument type and verify that the callbale
        // is invokable on that. This would let us detect errors early.

        // FRAZ: Get the value refs for each static arg.
        Box::new(PyUdfEvaluator {
            args,
            callable: self.callable.clone(),
        })
    }

    fn uuid(&self) -> &uuid::Uuid {
        &self.uuid
    }
}

impl Evaluator for PyUdfEvaluator {
    /// Evaluate the function with the given runtime info.
    fn evaluate(&self, work_area: &WorkArea<'_>) -> error_stack::Result<ArrayRef, Error> {
        let result = Python::with_gil(|py| -> PyResult<()> { evaluate_py(py, args) });
        // FRAZ: handle python errors.
        Ok(result)
    }
}

impl PyUdfEvaluator {
    fn evaluate_py<'py>(&self, py: Python<'py>, args: Vec<ArrayRef>) -> ArrayRef {
        // FRAZ: Fill in based on `call_udf`
        todo!()
    }
}

#[pymethods]
impl Udf {
    #[new]
    fn new(signature: String, callable: Py<PyAny>) -> Self {
        // FRAZ: Parse the signature and create an instance of PyUdf.
        // Then, create an `Arc<dyn Udf>` from that and pass that to
        // Udf.
        let udf = PyUdf {
            signature: todo!(),
            callable,
            uuid: todo!(),
        };
        Udf(Arc::new(udf))
    }
}

// #[pyfunction]
// #[pyo3(signature = (udf, result_type, *args))]
// pub(super) fn call_udf<'py>(
//     py: Python<'py>,
//     udf: &'py PyAny,
//     result_type: &'py PyAny,
//     args: &'py PyTuple,
// ) -> PyResult<&'py PyAny> {
//     let result_type = DataType::from_pyarrow(result_type)?;

//     // 1. Make sure we can convert each input to and from arrow arrays.
//     let mut udf_args = Vec::with_capacity(args.len() + 1);
//     udf_args.push(result_type.to_pyarrow(py)?);
//     for arg in args {
//         let array_data = ArrayData::from_pyarrow(arg)?;
//         let py_array: PyObject = array_data.to_pyarrow(py)?;
//         udf_args.push(py_array);
//     }
//     let args = PyTuple::new(py, udf_args);
//     let result = udf.call_method("run_pyarrow", args, None)?;

//     let array_data: ArrayData = ArrayData::from_pyarrow(result)?;
//     assert_eq!(array_data.data_type(), &result_type);

//     Ok(result)
// }
