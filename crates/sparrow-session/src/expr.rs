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
}

pub enum Literal {
    Null,
    String(String),
    Int64(i64),
    UInt64(u64),
    Float64(f64),
}
