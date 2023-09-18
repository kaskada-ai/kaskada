use arrow_array::ArrayRef;
use arrow_schema::DataType;
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
    output_type: DataType,
}

impl ExpressionExecutor {
    /// Create an `ExpressionExecutor` for the given expressions.
    pub fn try_new(
        input_type: &DataType,
        exprs: &[sparrow_physical::Expr],
    ) -> error_stack::Result<Self, Error> {
        let evaluators = evaluators::create_evaluators(input_type, exprs)?;
        let output_type = exprs
            .last()
            .expect("at least one expression")
            .result_type
            .clone();
        Ok(Self {
            evaluators,
            output_type,
        })
    }

    /// Execute the expressions on the given input batch.
    ///
    /// The result is a vector containing the results of each expression.
    pub fn execute(&self, input: &Batch) -> error_stack::Result<ArrayRef, Error> {
        let mut work_area = WorkArea::with_capacity(input, self.evaluators.len());
        for evaluator in self.evaluators.iter() {
            let output = evaluator.evaluate(&work_area)?;
            work_area.expressions.push(output);
        }
        Ok(work_area.expressions.pop().unwrap())
    }

    pub fn output_type(&self) -> &DataType {
        &self.output_type
    }
}
