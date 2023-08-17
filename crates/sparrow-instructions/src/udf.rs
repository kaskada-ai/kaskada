use sparrow_syntax::Signature;
use static_init::StaticInfo;

/// Defines the interface for user-defined functions.
trait UserDefinedFunction {
    fn signature(&self) -> &Signature;
    fn make_evaluator(&self, static_info: StaticInfo<'_>) -> Box<dyn Evaluator>;
}
