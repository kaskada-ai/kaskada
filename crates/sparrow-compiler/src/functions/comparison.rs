use sparrow_plan::InstOp;

use crate::functions::{Implementation, Registry};

pub(super) fn register(registry: &mut Registry) {
    registry
        .register("eq(a: any, b: any) -> bool")
        .with_implementation(Implementation::Instruction(InstOp::Eq));

    registry
        .register("neq(a: any, b: any) -> bool")
        .with_implementation(Implementation::Instruction(InstOp::Neq));

    registry
        .register("lt(a: ordered, b: ordered) -> bool")
        .with_implementation(Implementation::Instruction(InstOp::Lt));

    registry
        .register("gt(a: ordered, b: ordered) -> bool")
        .with_implementation(Implementation::Instruction(InstOp::Gt));

    registry
        .register("lte(a: ordered, b: ordered) -> bool")
        .with_implementation(Implementation::Instruction(InstOp::Lte));

    registry
        .register("gte(a: ordered, b: ordered) -> bool")
        .with_implementation(Implementation::Instruction(InstOp::Gte));
}
