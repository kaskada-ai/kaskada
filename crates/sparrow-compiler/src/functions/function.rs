use std::str::FromStr;
use std::sync::Arc;

use egg::{Subst, Var};
use itertools::{izip, Itertools};
use sparrow_api::kaskada::v1alpha::operation_plan::tick_operation::TickBehavior;
use sparrow_instructions::GroupId;
use sparrow_syntax::{FeatureSetPart, FenlType, Located, Location, Signature};

use crate::ast_to_dfg::AstDfg;
use crate::dfg::Dfg;
use crate::functions::implementation::Implementation;
use crate::functions::time_domain_check::TimeDomainCheck;
use crate::{AstDfgRef, DataContext, DiagnosticCollector};

/// Struct representing an instruction.
pub struct Function {
    /// The signature of this function as a string.
    signature_str: &'static str,
    /// Signature
    signature: Signature,
    /// Internal Signature for DFG
    internal_signature: Option<Signature>,
    /// How the function is implemented.
    implementation: Implementation,
    /// How the function determines `is_new`.
    is_new: Implementation,
    /// Configure how time domains are checked for the function.
    time_domain_check: TimeDomainCheck,
    /// Whether the function is internal only.
    internal: bool,
}

pub(super) struct FunctionBuilder<'building>(&'building mut Function);

impl<'building> FunctionBuilder<'building> {
    pub(super) fn new(instruction: &'building mut Function) -> Self {
        Self(instruction)
    }

    pub fn set_internal(self) -> Self {
        self.0.internal = true;
        self
    }

    pub fn with_implementation(self, implementation: Implementation) -> Self {
        self.0.implementation = implementation;
        self
    }

    pub fn with_is_new(self, is_new: Implementation) -> Self {
        self.0.is_new = is_new;
        self
    }

    pub fn with_time_domain_check(self, time_domain_check: TimeDomainCheck) -> Self {
        self.0.time_domain_check = time_domain_check;
        self
    }

    pub fn with_dfg_signature(self, signature_str: &'static str) -> Self {
        let signature =
            Signature::try_from_str(FeatureSetPart::Function(signature_str), signature_str)
                .unwrap_or_else(|e| panic!("Failed to parse signature '{signature_str}': {e:?}"));

        self.0.internal_signature = Some(signature);
        self
    }
}

impl Function {
    pub(super) fn new(signature: Signature, signature_str: &'static str) -> Self {
        Self {
            signature_str,
            signature,
            internal_signature: None,
            implementation: Implementation::Special,
            is_new: Implementation::AnyInputIsNew,
            time_domain_check: TimeDomainCheck::default(),
            internal: false,
        }
    }

    pub fn is_aggregation(&self) -> bool {
        // Note: Could add `is_agg` flag to registry
        matches!(
            self.name(),
            "sum"
                | "min"
                | "max"
                | "first"
                | "last"
                | "count"
                | "count_if"
                | "mean"
                | "variance"
                | "stddev"
        )
    }

    pub fn is_tick(&self) -> bool {
        matches!(self.implementation, Implementation::Tick(_))
    }

    /// Returns the tick behavior if this function is a tick.
    pub fn tick_behavior(&self) -> Option<TickBehavior> {
        match self.implementation {
            Implementation::Tick(b) => Some(b),
            _ => None,
        }
    }

    pub fn name(&self) -> &str {
        self.signature.name()
    }

    pub fn is_internal(&self) -> bool {
        self.internal
    }

    /// Returns the user-facing signature.
    pub fn signature(&self) -> &Signature {
        &self.signature
    }

    pub fn internal_signature(&self) -> &Signature {
        self.internal_signature.as_ref().unwrap_or(&self.signature)
    }

    pub fn signature_str(&self) -> &'static str {
        self.signature_str
    }

    pub(crate) fn arg_names(&self) -> impl Iterator<Item = &Located<String>> {
        if let Some(sig) = &self.internal_signature {
            sig.parameters().names().iter()
        } else {
            self.signature.parameters().names().iter()
        }
    }

    /// Create a substitution from the arguments to this function.
    ///
    /// Each parameter name `name` creates two entries -- `?name_is_new` and
    /// `?name_value`.
    ///
    /// If `skip` is `Some(n)` the `n`th argument is skipped. This works
    /// around the fact that you can't remove an argument. This is used by
    /// pushdown to create a substitution that skips the input that is recursed
    /// on.
    ///
    /// If `include_is_any_new` is `true`, the variable `?any_input_is_new`
    /// is bound to the logical conjunction of all `is_new` for all the
    /// arguments. This is optional since it creates DFG nodes that need to be
    /// simplified away, and just leads to cruft to create if it isn't needed.
    pub(super) fn create_subst_from_args(
        &self,
        dfg: &Dfg,
        args: &[Located<AstDfgRef>],
        skip: Option<usize>,
    ) -> Subst {
        // Create enough room to hold the *_is_new and *_value bindings for the
        // arguments plus a few more substitutions.
        //
        // - `3 * args.len()` since each argument has `?*_is_new`, `?*_value` and
        //   `?*_op`.
        // - `+ 5` to make room for `?_is_new`, `?input_field`, `?recurse_on_input` and
        //   `?input_record` from function pushdown.
        //
        // If this is too small the `Subst` will grow, so the above is a "best effort"
        // to avoid growth.
        let mut subst = Subst::with_capacity(3 * args.len() + 5);
        for (index, arg_name, argument) in izip!(0.., self.arg_names(), args) {
            if !skip.iter().contains(&index) {
                subst.insert(
                    Var::from_str(&format!("?{arg_name}_is_new")).expect("is_new var"),
                    argument.is_new(),
                );

                subst.insert(
                    Var::from_str(&format!("?{arg_name}_value")).expect("value var"),
                    argument.value(),
                );

                let operation = dfg.operation(argument.value());

                subst.insert(
                    Var::from_str(&format!("?{arg_name}_op")).expect("op var"),
                    operation,
                );
            }
        }

        subst
    }

    /// Expand the function to DFG nodes.
    // TODO: reduce number of arguments
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn create_dfg_node(
        &self,
        location: &Location,
        data_context: &mut DataContext,
        dfg: &mut Dfg,
        diagnostics: &mut DiagnosticCollector<'_>,
        args: &[Located<AstDfgRef>],
        value_type: FenlType,
        grouping: Option<GroupId>,
    ) -> anyhow::Result<AstDfgRef> {
        let is_new = self
            .is_new
            .create_node(data_context, self, dfg, diagnostics, args)?;

        let value = self
            .implementation
            .create_node(data_context, self, dfg, diagnostics, args)?;

        let time_domain =
            self.time_domain_check
                .check_args(location, diagnostics, args, data_context)?;

        Ok(Arc::new(AstDfg::new(
            value,
            is_new,
            value_type,
            grouping,
            time_domain,
            location.clone(),
            None,
        )))
    }
}

impl std::fmt::Debug for Function {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Instruction")
            .field("signature", &self.signature_str)
            .field("implementation", &self.implementation)
            .finish_non_exhaustive()
    }
}
