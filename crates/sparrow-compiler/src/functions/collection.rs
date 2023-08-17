use sparrow_instructions::InstOp;

use crate::functions::{Implementation, Registry};

use super::time_domain_check::TimeDomainCheck;

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
        .register("collect<T: any>(input: T, const max: i64, const min: i64 = 0, window: window = null) -> list<T>")
        .with_dfg_signature(
            "collect<T: any>(input: T, const max: i64, const min: i64 = 0, window: bool = null, duration: i64 = null) -> list<T>",
        )
        .with_implementation(Implementation::Instruction(InstOp::Collect))
        // This makes `collect` a continuous function. Though, it's not perhaps defined
        // as an aggregation, so we may want to rename or create a new category for it.
        .with_time_domain_check(TimeDomainCheck::Aggregation)
        .set_internal();

    registry
        .register("list_len<T: any>(input: list<T>) -> i32")
        .with_implementation(Implementation::Instruction(InstOp::ListLen))
        .set_internal();

    registry
        .register("flatten<T: any>(input: list<list<T>>) -> list<T>")
        .with_implementation(Implementation::Instruction(InstOp::Flatten))
        .set_internal();

    registry
        .register("union<T: any>(a: list<T>, b: list<T>) -> list<T>")
        .with_implementation(Implementation::Instruction(InstOp::Union))
        .set_internal();
}
