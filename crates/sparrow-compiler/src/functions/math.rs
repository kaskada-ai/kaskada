use sparrow_instructions::InstOp;

use crate::functions::{Implementation, Registry};

pub(super) fn register(registry: &mut Registry) {
    registry
        .register("neg<S: signed>(n: S) -> S")
        .with_implementation(Implementation::Instruction(InstOp::Neg));

    registry
        .register("ceil<N: number>(n: N) -> N")
        .with_implementation(Implementation::Instruction(InstOp::Ceil));

    registry
        .register("round<N: number>(n: N) -> N")
        .with_implementation(Implementation::Instruction(InstOp::Round));

    registry
        .register("floor<N: number>(n: N) -> N")
        .with_implementation(Implementation::Instruction(InstOp::Floor));

    registry
        .register("add<N: number>(a: N, b: N) -> N")
        .with_implementation(Implementation::Instruction(InstOp::Add));

    registry
        .register("sub<N: number>(a: N, b: N) -> N")
        .with_implementation(Implementation::Instruction(InstOp::Sub));

    registry
        .register("mul<N: number>(a: N, b: N) -> N")
        .with_implementation(Implementation::Instruction(InstOp::Mul));

    registry
        .register("div<N: number>(a: N, b: N) -> N")
        .with_implementation(Implementation::Instruction(InstOp::Div));

    registry
        .register("zip_min<O: ordered>(a: O, b: O) -> O")
        .with_implementation(Implementation::Instruction(InstOp::ZipMin));

    registry
        .register("zip_max<O: ordered>(a: O, b: O) -> O")
        .with_implementation(Implementation::Instruction(InstOp::ZipMax));

    registry
        .register("powf(base: f64, power: f64) -> f64")
        .with_implementation(Implementation::Instruction(InstOp::Powf));

    registry
        .register("sqrt<N: number>(a: N) -> f64")
        .with_implementation(Implementation::new_fenl_rewrite("powf(a, 0.5)"));

    registry
        .register("exp(power: f64) -> f64")
        .with_implementation(Implementation::Instruction(InstOp::Exp));

    registry
        .register("clamp<N: number>(value: N, min: N = null, max: N = null) -> N")
        .with_implementation(Implementation::Instruction(InstOp::Clamp));
}
