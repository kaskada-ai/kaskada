use std::sync::{Arc, Mutex};

use egg::Id;
use sparrow_instructions::GroupId;
use sparrow_syntax::{FenlType, Location};

use crate::time_domain::TimeDomain;

/// A non-thread safe reference to an `AstDfg`.
///
/// We may have multiple references to the same AstDfg node, so this allows us
/// to clone shallow references rather than deeply copy.
pub type AstDfgRef = Arc<AstDfg>;

/// Various information produced for each AST node during the conversion to the
/// DFG.
#[derive(Debug)]
pub struct AstDfg {
    /// Reference to the step containing the values of the AST node.
    ///
    /// Wrapped in a `RefCell` to allow mutability during
    /// pruning/simplification.
    pub(crate) value: Mutex<Id>,
    /// Reference to the step containing the "is_new" bits of the AST node.
    ///
    /// Wrapped in a `RefCell` to allow mutability during
    /// pruning/simplification.
    pub(crate) is_new: Mutex<Id>,
    /// Type of `value` produced.
    value_type: FenlType,
    /// Which entity grouping the node is associated with (if any).
    grouping: Option<GroupId>,
    /// Time Domain associated with the value. Used for reporting warnings.
    ///
    /// TODO: We could check the time domains in a separate pass, which would
    /// simplify the ast-to-dfg operation. We could do the various alignment,
    /// grouping, etc. checks all in that one place.
    time_domain: TimeDomain,
    /// The location of the expression that produced this `AstDfg`.
    ///
    /// NOTE: While the `Id`s are de-duplicated by `egg`, the `AstDfg` is not.
    /// Thus, if the user wrote `x + y` twice we would have two different
    /// `AstDfg` nodes with different locations but the same `value` `Id`.
    location: Location,
    /// If this AstDfg has known fields (e.g. is a struct literal) then this
    /// holds the location of each fields expression. The order matches
    // the order of fields in the type.
    fields: Option<Vec<AstDfgRef>>,
}

impl AstDfg {
    pub(crate) fn new(
        value: Id,
        is_new: Id,
        value_type: FenlType,
        grouping: Option<GroupId>,
        time_domain: TimeDomain,
        location: Location,
        fields: Option<Vec<AstDfgRef>>,
    ) -> AstDfg {
        #[cfg(debug_assertions)]
        if let Some(fields) = &fields {
            use arrow::datatypes::DataType;
            match &value_type {
                FenlType::Concrete(DataType::Struct(expected_fields)) => {
                    assert_eq!(
                        expected_fields.len(),
                        fields.len(),
                        "Expected equivalent number of fields and AST nodes"
                    );
                }
                unexpected => {
                    panic!("Had fields for non-struct type {unexpected:?}")
                }
            }
        };

        AstDfg {
            value: Mutex::new(value),
            is_new: Mutex::new(is_new),
            value_type,
            grouping,
            time_domain,
            location,
            fields,
        }
    }

    pub fn value(&self) -> Id {
        *self.value.lock().unwrap()
    }

    pub fn is_new(&self) -> Id {
        *self.is_new.lock().unwrap()
    }

    pub fn value_type(&self) -> &FenlType {
        &self.value_type
    }

    pub fn grouping(&self) -> Option<GroupId> {
        self.grouping
    }

    pub fn time_domain(&self) -> &TimeDomain {
        &self.time_domain
    }

    pub fn location(&self) -> &Location {
        &self.location
    }

    pub fn fields(&self) -> Option<&Vec<AstDfgRef>> {
        self.fields.as_ref()
    }
}
