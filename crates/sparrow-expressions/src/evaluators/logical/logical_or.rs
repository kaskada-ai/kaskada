use std::sync::Arc;

use arrow_array::ArrayRef;
use error_stack::{IntoReport, ResultExt};

use crate::evaluator::Evaluator;
use crate::evaluators::StaticInfo;
use crate::values::BooleanValue;
use crate::work_area::WorkArea;
use crate::Error;

inventory::submit!(crate::evaluators::EvaluatorFactory {
    name: "logical_or",
    create: &create
});

/// Evaluator for logical disjunction (`logical_or`).
///
/// This performs Kleene logic for three-valued logic. Returns `true` if either
/// side is `true`.
struct LogicalOrEvaluator {
    lhs: BooleanValue,
    rhs: BooleanValue,
}

impl Evaluator for LogicalOrEvaluator {
    fn evaluate(&self, info: &WorkArea<'_>) -> error_stack::Result<ArrayRef, Error> {
        let lhs = info.expression(self.lhs);
        let rhs = info.expression(self.rhs);

        let result = arrow_arith::boolean::or_kleene(lhs, rhs)
            .into_report()
            .change_context(Error::ExprEvaluation)?;
        Ok(Arc::new(result))
    }
}

fn create(info: StaticInfo<'_>) -> error_stack::Result<Box<dyn Evaluator>, Error> {
    let (lhs, rhs) = info.unpack_arguments()?;
    Ok(Box::new(LogicalOrEvaluator {
        lhs: lhs.boolean()?,
        rhs: rhs.boolean()?,
    }))
}
