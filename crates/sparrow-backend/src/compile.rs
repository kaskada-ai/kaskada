use std::borrow::Cow;

use crate::logical_to_physical::LogicalToPhysical;
use crate::Error;

/// Options for compiling logical plans to physical plans.
#[derive(Clone, Debug, Default)]
pub struct CompileOptions {}

/// Compile a logical plan to a physical execution plan.
pub fn compile(
    root: &sparrow_logical::ExprRef,
    options: Option<&CompileOptions>,
) -> error_stack::Result<sparrow_physical::Plan, Error> {
    let _options = if let Some(options) = options {
        Cow::Borrowed(options)
    } else {
        Cow::Owned(CompileOptions::default())
    };

    // Convert the logical expression tree to a physical plan.
    let mut physical = LogicalToPhysical::new().apply(root)?;

    // Schedule the steps in the physical plan.
    physical.pipelines = crate::pipeline_schedule(&physical.steps);

    Ok(physical)
}
