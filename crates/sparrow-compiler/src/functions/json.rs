use sparrow_instructions::InstOp;

use super::implementation::Implementation;
use crate::functions::Registry;

pub(super) fn register(registry: &mut Registry) {
    registry
        .register("json(s: string) -> json")
        .with_implementation(Implementation::Instruction(InstOp::Json))
        .set_internal();
}
