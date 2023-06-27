use sparrow_plan::InstOp;

use crate::functions::{Implementation, Registry};

pub(super) fn register(registry: &mut Registry) {
    registry
        .register("get(collection: collection<K, V>, key: K) -> V")
        .with_implementation(Implementation::Instruction(InstOp::Get));

}
