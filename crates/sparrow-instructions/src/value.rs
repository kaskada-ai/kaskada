use sparrow_arrow::scalar_value::ScalarValue;

/// Identifies a value to reference.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum ValueRef {
    /// References an input to the current operation by index.
    Input(u32),
    /// References an instruction in the current pass by index.
    Inst(u32),
    /// References a literal value.
    Literal(ScalarValue),
    /// References a tick in the current pass by index.
    Tick(u32),
}

impl ValueRef {
    pub fn is_literal_null(&self) -> bool {
        match self {
            ValueRef::Literal(literal) => literal.is_null(),
            _ => false,
        }
    }

    pub fn is_literal_value(&self, value: &ScalarValue) -> bool {
        match &self {
            ValueRef::Literal(literal) => literal == value,
            _ => false,
        }
    }

    pub fn is_literal(&self) -> bool {
        self.literal_value().is_some()
    }

    pub fn literal_value(&self) -> Option<&ScalarValue> {
        match self {
            ValueRef::Literal(literal) => Some(literal),
            _ => None,
        }
    }
}
