use std::sync::Arc;

use arrow_schema::{DataType, Schema};
use error_stack::ResultExt;
use futures::FutureExt;
use hashbrown::HashMap;
use sparrow_compiler::NearestMatches;
use sparrow_interfaces::source::Source;
use sparrow_interfaces::ExecutionOptions;
use sparrow_logical::{ExprRef, Literal};
use uuid::Uuid;

use crate::{Error, Execution};

/// Session for creating and executing partitioned queries.
pub struct Session {
    rt: tokio::runtime::Runtime,
    sources: HashMap<Uuid, Arc<dyn Source>>,
}

impl Default for Session {
    fn default() -> Self {
        Self {
            rt: tokio::runtime::Runtime::new().expect("session"),
            sources: HashMap::default(),
        }
    }
}

impl Session {
    pub fn add_source(&mut self, source: Arc<dyn Source>) -> error_stack::Result<ExprRef, Error> {
        let result_type = DataType::Struct(source.prepared_schema().fields.clone());

        let mut uuid = uuid::Uuid::new_v4();
        while self.sources.contains_key(&uuid) {
            uuid = uuid::Uuid::new_v4();
        }
        self.sources.insert(uuid, source);

        Ok(Arc::new(sparrow_logical::Expr::new_uuid(
            "read",
            uuid,
            result_type,
            // TODO: Managing groupings of sources.
            sparrow_logical::Grouping::new(0),
        )))
    }

    pub fn add_literal(&self, literal: Literal) -> error_stack::Result<ExprRef, Error> {
        Ok(Arc::new(sparrow_logical::Expr::new_literal(literal)))
    }

    pub fn add_expr(
        &self,
        function: &str,
        args: Vec<ExprRef>,
    ) -> error_stack::Result<ExprRef, Error> {
        // TODO: Would be good to intern the names early (here) to be `&'static str`.

        let function = match function {
            "record" => "record",
            "fieldref" => "fieldref",
            other => {
                // TODO(https://github.com/kaskada-ai/kaskada/issues/818): This
                // *should* use a list of available _logical_ functions. For
                // now, we approximate that with the set of available _physical_
                // functions.
                sparrow_interfaces::expression::intern_name(other).ok_or_else(|| {
                    Error::NoSuchFunction {
                        name: other.to_owned(),
                        nearest: NearestMatches::new_nearest_strings(
                            other,
                            sparrow_interfaces::expression::names().map(|s| s.to_owned()),
                        ),
                    }
                })?
            }
        };
        let expr = sparrow_logical::Expr::try_new(function, args).change_context(Error::Invalid)?;
        Ok(Arc::new(expr))
    }

    pub fn execute(
        &self,
        query: &ExprRef,
        execution_options: ExecutionOptions,
    ) -> error_stack::Result<Execution, Error> {
        let plan = sparrow_backend::compile(query, None).change_context(Error::Compile)?;

        let (output_tx, output_rx) = tokio::sync::mpsc::channel(10);
        let executor = sparrow_plan_execution::PlanExecutor::try_new(
            "query_id".to_owned(),
            plan,
            &self.sources,
            output_tx,
            Arc::new(execution_options),
        )
        .change_context(Error::Execute)?;

        let (stop_signal_tx, stop_signal_rx) = tokio::sync::watch::channel(false);

        let handle = self.rt.handle().clone();

        // TODO: Change `schema` to a `DataType` so we can output single columns.
        // This was a legacy constraint from old compilation / execution.
        let DataType::Struct(fields) = &query.result_type else {
            panic!("Change `schema` to `data_type` in execution and support primitive output")
        };
        let schema = Arc::new(Schema::new(fields.clone()));

        Ok(Execution::new(
            handle,
            output_rx,
            executor
                .execute(stop_signal_rx)
                .map(|result| result.change_context(Error::ExecutionFailed))
                .boxed(),
            stop_signal_tx,
            schema,
        ))
    }

    /// Allow running a future on this sessions runtime.
    ///
    /// This is currently only exposed for some tests which need to run
    /// futures adding data to sources without having a separate runtime.
    pub fn block_on<T>(&self, future: impl futures::Future<Output = T>) -> T {
        self.rt.block_on(future)
    }
}
