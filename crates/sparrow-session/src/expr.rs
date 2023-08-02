use arrow_schema::DataType;
use sparrow_compiler::AstDfgRef;
use sparrow_syntax::FenlType;

#[derive(Clone, Debug)]
pub struct Expr(pub(crate) AstDfgRef);

impl Expr {
    pub fn data_type(&self) -> Option<&DataType> {
        match self.0.value_type() {
            FenlType::Concrete(data_type) => Some(data_type),
            _ => None,
        }
    }

    pub fn equivalent(&self, other: &Expr) -> bool {
        // This isn't quite correct -- we should lock everything and then compare.
        // But, this is a temporary hack for the Python builder.
        self.0.value() == other.0.value()
            && self.0.is_new() == other.0.is_new()
            && self.0.value_type() == other.0.value_type()
            && self.0.grouping() == other.0.grouping()
            && self.0.time_domain() == other.0.time_domain()
    }
}

pub enum Literal {
    Null,
    String(String),
    Int64(i64),
    UInt64(u64),
    Float64(f64),
}
