use std::sync::Arc;

use arrow_array::{ArrayRef, ArrowNumericType};
use error_stack::{IntoReport, ResultExt};

use sparrow_interfaces::expression::{Error, Evaluator, PrimitiveValue, StaticInfo, WorkArea};

inventory::submit!(sparrow_interfaces::expression::EvaluatorFactory {
    name: "gt_primitive",
    create: &crate::macros::create_primitive_evaluator!(0, create, ordered)
});

/// Evaluator for the `gt_primitive` (greater than) instruction.
struct GtPrimitiveEvaluator<T: ArrowNumericType> {
    lhs: PrimitiveValue<T>,
    rhs: PrimitiveValue<T>,
}

impl<T: ArrowNumericType> Evaluator for GtPrimitiveEvaluator<T> {
    fn evaluate(&self, work_area: &WorkArea<'_>) -> error_stack::Result<ArrayRef, Error> {
        let lhs = work_area.expression(self.lhs);
        let rhs = work_area.expression(self.rhs);

        #[allow(deprecated)] // https://github.com/kaskada-ai/kaskada/issues/783
        let result = arrow_ord::comparison::gt::<T>(lhs, rhs)
            .into_report()
            .change_context(Error::ExprEvaluation)?;
        Ok(Arc::new(result))
    }
}

fn create<T: ArrowNumericType>(
    info: StaticInfo<'_>,
) -> error_stack::Result<Box<dyn Evaluator>, Error> {
    let (lhs, rhs) = info.unpack_arguments()?;

    // We don't need to check the data type -- the `lhs.primitive()` and `rhs.primitive()` will
    // confirm that is compatible with the primitive type.
    Ok(Box::new(GtPrimitiveEvaluator::<T> {
        lhs: lhs.primitive()?,
        rhs: rhs.primitive()?,
    }))
}
