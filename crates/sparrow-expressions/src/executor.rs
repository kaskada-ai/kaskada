use arrow_array::ArrayRef;
use arrow_schema::Schema;
use sparrow_arrow::Batch;

use crate::evaluator::Evaluator;
use crate::evaluators;
use crate::work_area::WorkArea;
use crate::Error;

/// Executes the expressions within an operation.
///
/// Each operation produces a stream of inputs (`BoxedInputBatch`).
/// Expressions create columns from the input and by evaluating instructions
/// against existing columns.
pub struct ExpressionExecutor {
    evaluators: Vec<Box<dyn Evaluator>>,
}

impl ExpressionExecutor {
    /// Create an `ExpressionExecutor` for the given expressions.
    pub fn try_new(
        input_schema: &Schema,
        exprs: &[sparrow_physical::Expr],
    ) -> error_stack::Result<Self, Error> {
        let evaluators = evaluators::create_evaluators(input_schema, exprs)?;
        Ok(Self { evaluators })
    }

    /// Execute the expressions on the given input batch.
    ///
    /// The result is a vector containing the results of each expression.
    pub fn execute(&self, input: &Batch) -> error_stack::Result<Vec<ArrayRef>, Error> {
        let mut work_area = WorkArea::with_capacity(input, self.evaluators.len());
        for evaluator in self.evaluators.iter() {
            let output = evaluator.evaluate(&work_area)?;
            work_area.expressions.push(output);
        }
        Ok(work_area.expressions)
    }
}
