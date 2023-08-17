use std::str::FromStr;

use anyhow::{anyhow, Context};
use egg::{Id, Var};
use itertools::izip;
use once_cell::sync::OnceCell;
use smallvec::smallvec;
use sparrow_api::kaskada::v1alpha::operation_plan::tick_operation::TickBehavior;
use sparrow_instructions::InstOp;
use sparrow_syntax::{Expr, FeatureSetPart, FenlType, Located, ResolvedExpr, WindowBehavior};

use crate::ast_to_dfg::ast_to_dfg;
use crate::dfg::{Dfg, DfgPattern, Operation, StepKind};
use crate::frontend::resolve_arguments::resolve_recursive;
use crate::functions::{Function, Pushdown};
use crate::{is_any_new, AstDfgRef, DataContext, DiagnosticCollector};

/// Enum describing how a function is implemented.
#[derive(Debug)]
pub(super) enum Implementation {
    /// This function has special handling in `ast_to_dfg`.
    Special,
    /// This function compiles directly to the given instruction.
    Instruction(InstOp),
    /// This function creates a window behavior.
    Window(WindowBehavior),
    /// The function should be expanded using the given pattern.
    Pattern(DfgPattern),
    /// The function should be expanded on primitive fields using the given
    /// pushdown.
    Pushdown(Box<Pushdown>),
    /// The function should be rewritten as the given fenl expression.
    ///
    /// This differs from `Rewrite` in that this expression uses fenl syntax and
    /// is expanded and simplified as normal.
    ///
    /// We shouldn't need to use a Box, but we need a fixed size function.
    /// Even though we're creating a closure over always the same thing
    /// (static string reference), the compiler doesn't currently know
    /// that that is fixed size -- so we `Box` it.
    FenlBody {
        expression: &'static str,
        resolved: OnceCell<Box<ResolvedExpr>>,
    },
    /// Tick Behavior
    Tick(TickBehavior),
    /// Produce a boolean DFG which is true if any of the inputs `is_new`.
    ///
    /// Produces a DFG node corresponding to `(logical_or ...)`  applied to
    /// all of the input `is_new` values.
    AnyInputIsNew,
}

impl Implementation {
    pub(super) fn new_pattern(pattern: &str) -> Self {
        // TODO: Validate implementations against the signature?
        // Could use this to (a) report the parse errors better and (b)
        // ensure the variables in the pattern are bound.
        let pattern = DfgPattern::from_str(pattern).context("rewrite").unwrap();
        Self::Pattern(pattern)
    }

    /// Rewrites in the form of fenl expressions.
    ///
    /// For example, `sqrt(n)` is rewritten as `powf(n, 0.5)`, which allows the
    /// reuse of existing kernels.
    pub(super) fn new_fenl_rewrite(expression: &'static str) -> Self {
        Self::FenlBody {
            expression,
            resolved: OnceCell::new(),
        }
    }

    pub(super) fn create_node(
        &self,
        data_context: &mut DataContext,
        function: &Function,
        dfg: &mut Dfg,
        diagnostics: &mut DiagnosticCollector<'_>,
        args: &[Located<AstDfgRef>],
    ) -> anyhow::Result<Id> {
        match self {
            Implementation::Special => Err(anyhow!(
                "Special function '{:?}' should have been handled specially",
                function.name(),
            )),
            Implementation::Instruction(inst) => {
                Ok(dfg.add_instruction(*inst, args.iter().map(|i| i.value()).collect())?)
            }
            Implementation::Tick(tick) => {
                Ok(dfg.add_operation(Operation::Tick(*tick), smallvec![])?)
            }
            Implementation::Window(window) => Ok(dfg.add_node(
                StepKind::Window(*window),
                args.iter().map(|i| i.value()).collect(),
            )?),
            Implementation::Pattern(pattern) => {
                // This subst may have already been created for `is_new`. That may be
                // inefficient. That probably doesn't matter.
                let subst = function.create_subst_from_args(dfg, args, None);
                dfg.add_pattern(pattern, &subst)
            }
            Implementation::FenlBody {
                expression,
                resolved,
            } => {
                let resolved = resolved.get_or_try_init(|| -> anyhow::Result<_> {
                    // If the implementation of a function is rewritten (as defined in the
                    // registry), we'll produce the dfg for the corresponding
                    // call here. Rewrites are internal definitions, so errors
                    // parsing or resolving the expression should be reported as
                    // internal errors.
                    let part = FeatureSetPart::Function(expression);
                    let expr = Expr::try_from_str(part, expression).map_err(|e| {
                        anyhow::anyhow!("Invalid fenl body: '{}' -- {:?}", expression, e)
                    })?;

                    let mut diagnostics = Vec::new();
                    let expr = resolve_recursive(&expr, &mut diagnostics)
                        .with_context(|| format!("Failed to to resolve expr: {expression:?}"))?;

                    anyhow::ensure!(
                        diagnostics.is_empty(),
                        "Encountered error resolving expr: {:?}",
                        diagnostics
                    );

                    Ok(Box::new(expr))
                })?;

                dfg.enter_env();

                let parameter_names = function.signature().parameters().names();
                for (parameter, value) in izip!(parameter_names, args) {
                    dfg.bind(parameter.inner(), value.inner().clone());
                }

                let result = ast_to_dfg(data_context, dfg, diagnostics, resolved.as_ref())?;
                dfg.exit_env();

                Ok(result.value())
            }
            Implementation::Pushdown(pushdown) => {
                // To avoid accidents, we don't include the "driving" argument in the
                // substitution. Specifically, the "input" to the pushdown will
                // be changed at each recursion.
                let mut subst =
                    function.create_subst_from_args(dfg, args, Some(pushdown.pushdown_on()));

                let pushdown_on = &args[pushdown.pushdown_on()];
                // Add an `is_new` that indicates whether the argument being pushed down on
                // was new. We can't access the `is_new` of individual components.
                subst.insert(
                    Var::from_str("?is_new").context("Failed to parse ?is_new")?,
                    pushdown_on.is_new(),
                );
                subst.insert(
                    Var::from_str("?op").context("Failed to parse ?op")?,
                    dfg.operation(pushdown_on.value()),
                );

                match pushdown_on.value_type() {
                    FenlType::Concrete(data_type) => {
                        pushdown.pushdown(dfg, &subst, pushdown_on.value(), data_type)
                    }
                    FenlType::Error => Ok(dfg.error_node().value()),
                    non_concrete => Err(anyhow!(
                        "Unable to pushdown '{}' on non-concrete type {}",
                        function.name(),
                        non_concrete
                    )),
                }
            }
            Implementation::AnyInputIsNew => Ok(is_any_new(dfg, args)?),
        }
    }
}
