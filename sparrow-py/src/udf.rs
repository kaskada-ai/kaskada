use arrow::array::ArrayData;
use arrow::datatypes::DataType;
use arrow::pyarrow::{FromPyArrow, ToPyArrow};
use pyo3::prelude::*;
use pyo3::types::PyTuple;

#[pyfunction]
#[pyo3(signature = (udf, result_type, *args))]
pub(super) fn call_udf<'py>(
    py: Python<'py>,
    udf: &'py PyAny,
    result_type: &'py PyAny,
    args: &'py PyTuple,
) -> PyResult<&'py PyAny> {
    let result_type = DataType::from_pyarrow(result_type)?;

    // 1. Make sure we can convert each input to and from arrow arrays.
    let mut udf_args = Vec::with_capacity(args.len() + 1);
    udf_args.push(result_type.to_pyarrow(py)?);
    for arg in args {
        let array_data = ArrayData::from_pyarrow(arg)?;
        let py_array: PyObject = array_data.to_pyarrow(py)?;
        udf_args.push(py_array);
    }
    let args = PyTuple::new(py, udf_args);
    let result = udf.call_method("run_pyarrow", args, None)?;

    let array_data: ArrayData = ArrayData::from_pyarrow(result)?;
    assert_eq!(array_data.data_type(), &result_type);

    Ok(result)
}
