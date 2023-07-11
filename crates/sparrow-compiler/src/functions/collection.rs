use sparrow_plan::InstOp;

use crate::functions::{Implementation, Registry};

pub(super) fn register(registry: &mut Registry) {
    registry
        .register("get<K: key, V: any>(key: K, map: map<K, V>) -> V")
        .with_implementation(Implementation::Instruction(InstOp::Get))
        .set_internal();
}
