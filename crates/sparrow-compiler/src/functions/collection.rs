use sparrow_plan::InstOp;

use crate::functions::{Implementation, Registry};

pub(super) fn register(registry: &mut Registry) {
    registry
        .register("get<K: key, V: any>(key: K, map: map<K, V>) -> V")
        .with_implementation(Implementation::Instruction(InstOp::Get))
        .set_internal();

    registry
        .register("index<T: any>(i: i64, list: list<T>) -> T")
        .with_implementation(Implementation::Instruction(InstOp::Index))
        .set_internal();

    registry
        .register("collect<T: any>(const max: i64, input: T, window: window = null) -> list<T>")
        .with_dfg_signature(
            "collect<T: any>(const max: i64, input: T, window: bool = null, duration: i64 = null) -> list<T>",
        )
        .with_implementation(Implementation::Instruction(InstOp::Collect))
        .set_internal();

    registry
        .register("list_len<T: any>(input: list<T>) -> i32")
        .with_implementation(Implementation::Instruction(InstOp::ListLen))
        .set_internal();
}
