use std::sync::Arc;

use arrow_array::ArrayRef;
use error_stack::{IntoReport, ResultExt};

use sparrow_interfaces::expression::{BooleanValue, Error, Evaluator, StaticInfo, WorkArea};

inventory::submit!(sparrow_interfaces::expression::EvaluatorFactory {
    name: "logical_and",
    create: &create
});

/// Evaluator for logical conjunction (`logical_and`).
///
/// This performs Kleene logic for three-valued logic. Returns `false` if either
/// side is `false`.
struct LogicalAndEvaluator {
    lhs: BooleanValue,
    rhs: BooleanValue,
}

impl Evaluator for LogicalAndEvaluator {
    fn evaluate(&self, info: &WorkArea<'_>) -> error_stack::Result<ArrayRef, Error> {
        let lhs = info.expression(self.lhs);
        let rhs = info.expression(self.rhs);

        let result = arrow_arith::boolean::and_kleene(lhs, rhs)
            .into_report()
            .change_context(Error::ExprEvaluation)?;
        Ok(Arc::new(result))
    }
}

fn create(info: StaticInfo<'_>) -> error_stack::Result<Box<dyn Evaluator>, Error> {
    let (lhs, rhs) = info.unpack_arguments()?;
    Ok(Box::new(LogicalAndEvaluator {
        lhs: lhs.boolean()?,
        rhs: rhs.boolean()?,
    }))
}
