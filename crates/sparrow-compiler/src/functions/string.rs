use sparrow_instructions::InstOp;

use crate::functions::{Implementation, Registry};

pub(super) fn register(registry: &mut Registry) {
    registry
        .register("len(s: string) -> i32")
        .with_implementation(Implementation::Instruction(InstOp::Len));

    registry
        .register("upper(s: string) -> string")
        .with_implementation(Implementation::Instruction(InstOp::Upper));

    registry
        .register("lower(s: string) -> string")
        .with_implementation(Implementation::Instruction(InstOp::Lower));

    registry
        .register("substring(s: string, start: i64 = null, end: i64 = null) -> string")
        .with_implementation(Implementation::Instruction(InstOp::Substring));
}
