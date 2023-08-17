use sparrow_instructions::InstOp;

use crate::functions::{Implementation, Registry};

pub(super) fn register(registry: &mut Registry) {
    registry
        .register("eq<T: any>(a: T, b: T) -> bool")
        .with_implementation(Implementation::Instruction(InstOp::Eq));

    registry
        .register("neq<T: any>(a: T, b: T) -> bool")
        .with_implementation(Implementation::Instruction(InstOp::Neq));

    registry
        .register("lt<O: ordered>(a: O, b: O) -> bool")
        .with_implementation(Implementation::Instruction(InstOp::Lt));

    registry
        .register("gt<O: ordered>(a: O, b: O) -> bool")
        .with_implementation(Implementation::Instruction(InstOp::Gt));

    registry
        .register("lte<O: ordered>(a: O, b: O) -> bool")
        .with_implementation(Implementation::Instruction(InstOp::Lte));

    registry
        .register("gte<O: ordered>(a: O, b: O) -> bool")
        .with_implementation(Implementation::Instruction(InstOp::Gte));
}
