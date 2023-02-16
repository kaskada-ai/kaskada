use sparrow_plan::InstOp;

use crate::functions::{Implementation, Registry};

pub(super) fn register(registry: &mut Registry) {
    registry
        .register("not(input: bool) -> bool")
        .with_implementation(Implementation::Instruction(InstOp::Not));

    registry
        .register("logical_or(a: bool, b: bool) -> bool")
        .with_implementation(Implementation::Instruction(InstOp::LogicalOr));

    registry
        .register("logical_and(a: bool, b: bool) -> bool")
        .with_implementation(Implementation::Instruction(InstOp::LogicalAnd));

    registry
        .register("if(condition: bool, value: any) -> any")
        .with_implementation(Implementation::Instruction(InstOp::If));

    registry
        .register("null_if(condition: bool, value: any) -> any")
        .with_implementation(Implementation::Instruction(InstOp::NullIf));

    registry
        .register("else(default: any, value: any) -> any")
        .with_implementation(Implementation::new_fenl_rewrite("coalesce(value, default)"));
}
