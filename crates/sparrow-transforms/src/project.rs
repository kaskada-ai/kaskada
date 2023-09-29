use arrow_schema::DataType;
use error_stack::ResultExt;
use sparrow_arrow::Batch;

use sparrow_expressions::ExpressionExecutor;
use sparrow_physical::Exprs;

use crate::transform::{Error, Transform};

/// Transform for projection.
pub struct Project {
    evaluators: ExpressionExecutor,
}

impl Project {
    pub fn try_new(
        input_type: &DataType,
        exprs: &Exprs,
        output_type: &DataType,
    ) -> error_stack::Result<Self, Error> {
        let evaluators = ExpressionExecutor::try_new(input_type, exprs.as_vec())
            .change_context_lazy(|| Error::CreateTransform("project"))?;
        error_stack::ensure!(
            output_type == evaluators.output_type(),
            Error::MismatchedResultType {
                transform: "project",
                expected: output_type.clone(),
                actual: evaluators.output_type().clone()
            }
        );
        Ok(Self { evaluators })
    }
}

impl Transform for Project {
    fn apply(&self, batch: Batch) -> error_stack::Result<Batch, Error> {
        assert!(!batch.is_empty());

        let error = || Error::ExecuteTransform("project");
        let result = self.evaluators.execute(&batch).change_context_lazy(error)?;
        Ok(batch.with_projection(result))
    }

    fn name(&self) -> &'static str {
        std::any::type_name::<Self>()
    }
}
