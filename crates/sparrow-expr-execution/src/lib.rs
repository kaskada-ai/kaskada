#![warn(
    rust_2018_idioms,
    nonstandard_style,
    future_incompatible,
    clippy::mod_module_files,
    clippy::print_stdout,
    clippy::print_stderr,
    clippy::undocumented_unsafe_blocks
)]

//! Execution of expressions.
//!
//! Creating an [ExpressionExecutor] takes a sequence of
//! [expressions][sparrow_physical::Expr] and applies them to an input batch to
//! produce a sequence of computed columns. Expressions may reference columns
//! from the input to the expressions (typically the input to the step executing
//! the expressions), create a column from a literal value, or apply an
//! expression to columns computed by earlier expressions.

use arrow_array::ArrayRef;
use arrow_schema::DataType;
use index_vec::IndexVec;
use sparrow_batch::Batch;

use sparrow_interfaces::expression::{Error, Evaluator, StaticArg, StaticInfo, WorkArea};

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
    pub fn try_new(exprs: &[sparrow_physical::Expr]) -> error_stack::Result<Self, Error> {
        let evaluators = create_evaluators(exprs)?;
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

/// Create the evaluators for the given expressions.
fn create_evaluators(
    exprs: &[sparrow_physical::Expr],
) -> error_stack::Result<Vec<Box<dyn Evaluator>>, Error> {
    // Static information (index in expressions, type, etc.) for each expression in `exprs`.
    // This is used to locate the information about arguments to the remaining expressions.
    //
    // It is only needed while instantiating the evaluators.
    let mut expressions = IndexVec::with_capacity(exprs.len());
    let mut evaluators = Vec::with_capacity(exprs.len());
    for (index, expr) in exprs.iter().enumerate() {
        let args = expr.args.iter().map(|index| &expressions[*index]).collect();
        let info = StaticInfo {
            name: &expr.name,
            literal_args: &expr.literal_args,
            args,
            result_type: &expr.result_type,
        };

        evaluators.push(sparrow_interfaces::expression::create_evaluator(info)?);
        expressions.push(StaticArg {
            index,
            data_type: &expr.result_type,
        });
    }
    Ok(evaluators)
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_expressions_registered() {
        sparrow_expressions::ensure_registered();
    }
}
