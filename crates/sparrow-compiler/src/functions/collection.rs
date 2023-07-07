use sparrow_plan::InstOp;

use crate::functions::{Implementation, Registry};

pub(super) fn register(registry: &mut Registry) {
    registry
        .register("get<K: key, V: any>(map: map<K, V>, key: K) -> V")
        .with_implementation(Implementation::Instruction(InstOp::Get));
}
