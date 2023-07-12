use arrow_array::ArrayRef;

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
    _map: MapValue,
    _key: ArrayRefValue,
}

impl Evaluator for GetEvaluator {
    fn evaluate(
        &self,
        _work_area: &crate::work_area::WorkArea<'_>,
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
