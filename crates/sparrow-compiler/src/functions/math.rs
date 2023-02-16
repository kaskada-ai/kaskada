use sparrow_plan::InstOp;

use crate::functions::{Implementation, Registry};

pub(super) fn register(registry: &mut Registry) {
    registry
        .register("neg(n: signed) -> signed")
        .with_implementation(Implementation::Instruction(InstOp::Neg));

    registry
        .register("ceil(n: number) -> number")
        .with_implementation(Implementation::Instruction(InstOp::Ceil));

    registry
        .register("round(n: number) -> number")
        .with_implementation(Implementation::Instruction(InstOp::Round));

    registry
        .register("floor(n: number) -> number")
        .with_implementation(Implementation::Instruction(InstOp::Floor));

    registry
        .register("add(a: number, b: number) -> number")
        .with_implementation(Implementation::Instruction(InstOp::Add));

    registry
        .register("sub(a: number, b: number) -> number")
        .with_implementation(Implementation::Instruction(InstOp::Sub));

    registry
        .register("mul(a: number, b: number) -> number")
        .with_implementation(Implementation::Instruction(InstOp::Mul));

    registry
        .register("div(a: number, b: number) -> number")
        .with_implementation(Implementation::Instruction(InstOp::Div));

    registry
        .register("zip_min(a: ordered, b: ordered) -> ordered")
        .with_implementation(Implementation::Instruction(InstOp::ZipMin));

    registry
        .register("zip_max(a: ordered, b: ordered) -> ordered")
        .with_implementation(Implementation::Instruction(InstOp::ZipMax));

    registry
        .register("powf(base: f64, power: f64) -> f64")
        .with_implementation(Implementation::Instruction(InstOp::Powf));

    registry
        .register("sqrt(a: number) -> f64")
        .with_implementation(Implementation::new_fenl_rewrite("powf(a, 0.5)"));

    registry
        .register("exp(power: f64) -> f64")
        .with_implementation(Implementation::Instruction(InstOp::Exp));

    registry
        .register("clamp(value: number, min: number = null, max: number = null) -> number")
        .with_implementation(Implementation::Instruction(InstOp::Clamp));
}
