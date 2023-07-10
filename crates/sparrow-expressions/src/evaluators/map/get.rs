use arrow_array::{ArrayRef, BooleanArray};
use error_stack::{IntoReport, ResultExt};
use sparrow_arrow::downcast::downcast_primitive_array;

use crate::evaluator::Evaluator;
use crate::evaluators::StaticInfo;
use crate::values::{ArrayRefValue, MapValue};
use crate::Error;

inventory::submit!(crate::evaluators::EvaluatorFactory {
    name: "get",
    create: &create
});

/// Evaluator for `get` on maps.
struct GetEvaluator {
    map: MapValue,
    key: ArrayRefValue,
}

impl Evaluator for GetEvaluator {
    fn evaluate(
        &self,
        work_area: &crate::work_area::WorkArea<'_>,
    ) -> error_stack::Result<ArrayRef, Error> {
        todo!("unimplemented");
    }
}

fn create(info: StaticInfo<'_>) -> error_stack::Result<Box<dyn Evaluator>, Error> {
    error_stack::ensure!(
        !info.args.is_empty(),
        Error::NoArguments {
            name: info.name.clone(),
            actual: info.args.len()
        }
    );

    todo!("unimplemented");
}
