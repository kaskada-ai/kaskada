use std::fmt::Debug;
use std::sync::Arc;
use itertools::Itertools;

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
    uuid: uuid::Uuid,
}

struct PyUdfEvaluator {
    args: Vec<ValueRef>,
    callable: Py<PyAny>,
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
        println!("Making udf evaluator");
        // Idea: Create a random value of each argument type and verify that the callbale
        // is invokable on that. This would let us detect errors early.

        let args = info.args.iter().map(|arg| arg.value_ref.clone()).collect();

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
    fn evaluate(&mut self, info: &dyn RuntimeInfo) -> anyhow::Result<ArrayRef> {
        let args = self.args
            .iter()
            .map(|value_ref| -> anyhow::Result<_> {
                Ok(info.value(value_ref)?.array_ref()?)
            }).try_collect()?;
 
        let result = Python::with_gil(|py| -> anyhow::Result<ArrayRef> {
            // todo: uhh
            Ok(self.evaluate_py(py, args))
        })?;
        // FRAZ: handle python errors.
        Ok(result)
    }
}

impl PyUdfEvaluator {
    fn evaluate_py<'py>(&self, py: Python<'py>, args: Vec<ArrayRef>) -> ArrayRef {
        println!("Evaluating python");

        // self.callable.call1(py, (args,)).unwrap().extract(py).unwrap();
        // let args: Vec<PyObject> = args 
        //     .iter()
        //     .map::<anyhow::Result<_>, _>(|value_ref| {
        //         let value: ColumnarValue = info.value(value_ref)?;
        //         let array_ref: ArrayRef = value.array_ref()?;
        //         let array_data: ArrayData = aray_ref.to_data();
        //         let py_obj: PyObject = array_data.to_pyarrow(py)?;

        //         Ok(py_obj)
        //     })
        //     .collect::<anyhow::Result<_>>()?;

        // let locals = [("args", args)].into_py_dict(py);
        // py.run(&self.code, None, Some(&locals))?;
        // let result_py: PyObject = locals.get_item("ret").unwrap().into();

        // let result_array_data: ArrayData = ArrayData::from_pyarrow(result_py.as_ref(py))?;

        // // We can't control the returned type, but we can refuse to accept the "wrong" type.
        // ensure!(
        //     *result_array_data.data_type() == self.result_type,
        //     "expected Python UDF to return type {:?}, but received {:?}",
        //     result_array_data.data_type(),
        //     self.result_type
        // );

        // let result_array_ref: ArrayRef = array::make_array(result_array_data);

        // // This is a "map" operation - each row should reflect the time of the
        // // corresponding input row, and have the same length.
        // ensure!(
        //     result_array_ref.len() == info.num_rows(),
        //     "unexpected result length from Python UDF"
        // );

        panic!("arrayref")
    }
}

#[pymethods]
impl Udf {
    #[new]
    #[pyo3(signature = (signature, callable))]
    fn new(signature: String, callable: Py<PyAny>) -> Self {
        let uuid = Uuid::new_v4();
        // TODO: sig name string cannot be &'static str
        let signature = Signature::try_from_str(FeatureSetPart::Function("udf"), &signature).expect("signature to parse");
        let udf = PyUdf {
            signature,
            callable,
            uuid,
        };
        Udf(Arc::new(udf))
    }
}
