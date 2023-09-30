use std::sync::Arc;

use arrow_array::{ArrayRef, ArrowNumericType};
use error_stack::{IntoReport, ResultExt};

use crate::evaluator::Evaluator;
use crate::evaluators::StaticInfo;
use crate::values::PrimitiveValue;
use crate::work_area::WorkArea;
use crate::Error;

inventory::submit!(crate::evaluators::EvaluatorFactory {
    name: "sub",
    create: &crate::evaluators::macros::create_primitive_evaluator!(0, create, number)
});

/// Evaluator for subtraction (`sub`).
struct SubEvaluator<T: ArrowNumericType> {
    lhs: PrimitiveValue<T>,
    rhs: PrimitiveValue<T>,
}

impl<T: ArrowNumericType> Evaluator for SubEvaluator<T> {
    fn evaluate(&self, info: &WorkArea<'_>) -> error_stack::Result<ArrayRef, Error> {
        let lhs = info.expression(self.lhs);
        let rhs = info.expression(self.rhs);
        let result = arrow_arith::numeric::sub_wrapping(lhs, rhs)
            .into_report()
            .change_context(Error::ExprEvaluation)?;
        Ok(Arc::new(result))
    }
}

fn create<T: ArrowNumericType>(
    info: StaticInfo<'_>,
) -> error_stack::Result<Box<dyn Evaluator>, Error> {
    let (lhs, rhs) = info.unpack_arguments()?;
    Ok(Box::new(SubEvaluator::<T> {
        lhs: lhs.primitive()?,
        rhs: rhs.primitive()?,
    }))
}
