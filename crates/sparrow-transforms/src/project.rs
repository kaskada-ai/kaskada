use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use error_stack::{IntoReport, ResultExt};
use sparrow_arrow::Batch;

use sparrow_expressions::ExpressionExecutor;
use sparrow_physical::Exprs;

use crate::transform::{Error, Transform};

/// Transform for projection.
pub struct Project {
    evaluators: ExpressionExecutor,
    outputs: Vec<usize>,
    schema: SchemaRef,
}

impl Project {
    pub fn try_new(
        input_schema: &SchemaRef,
        exprs: &Exprs,
        schema: SchemaRef,
    ) -> error_stack::Result<Self, Error> {
        let evaluators = ExpressionExecutor::try_new(input_schema.as_ref(), exprs.exprs.as_vec())
            .change_context_lazy(|| Error::CreateTransform("project"))?;
        Ok(Self {
            evaluators,
            outputs: exprs.outputs.iter().map(|n| (*n).into()).collect(),
            schema,
        })
    }
}

impl Transform for Project {
    fn apply(&self, batch: Batch) -> error_stack::Result<Batch, Error> {
        assert!(!batch.is_empty());

        let error = || Error::ExecuteTransform("project");
        let columns = self.evaluators.execute(&batch).change_context_lazy(error)?;
        let columns = self
            .outputs
            .iter()
            .map(|index| columns[*index].clone())
            .collect();

        let result = RecordBatch::try_new(self.schema.clone(), columns)
            .into_report()
            .change_context_lazy(error)?;
        Ok(batch.with_projection(result))
    }

    fn name(&self) -> &'static str {
        std::any::type_name::<Self>()
    }
}
