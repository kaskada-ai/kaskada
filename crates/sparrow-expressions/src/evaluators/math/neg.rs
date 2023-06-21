use std::sync::Arc;

use arrow_array::{ArrayRef, ArrowNumericType};
use error_stack::{IntoReport, ResultExt};

use crate::evaluator::Evaluator;
use crate::evaluators::StaticInfo;
use crate::values::PrimitiveValue;
use crate::work_area::WorkArea;
use crate::Error;

inventory::submit!(crate::evaluators::EvaluatorFactory {
    name: "neg",
    create: &crate::evaluators::macros::create_primitive_evaluator!(0, create, signed)
});

/// Evaluator for unary negation.
struct NegEvaluator<T: ArrowNumericType> {
    input: PrimitiveValue<T>,
}

impl<T: ArrowNumericType> Evaluator for NegEvaluator<T> {
    fn evaluate(&self, info: &WorkArea<'_>) -> error_stack::Result<ArrayRef, Error> {
        let input = info.expression(self.input);
        let result = arrow_arith::arithmetic::negate::<T>(input)
            .into_report()
            .change_context(Error::ExprEvaluation)?;
        Ok(Arc::new(result))
    }
}

fn create<T: ArrowNumericType>(
    info: StaticInfo<'_>,
) -> error_stack::Result<Box<dyn Evaluator>, Error> {
    let input = info.unpack_argument()?;
    Ok(Box::new(NegEvaluator::<T> {
        input: input.primitive()?,
    }))
}
