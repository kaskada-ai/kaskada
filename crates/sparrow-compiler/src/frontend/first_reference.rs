use itertools::izip;
use sparrow_syntax::{ExprOp, Located, Resolved, ResolvedExpr};

/// Return the first free reference to the `needle`.
///
/// This returns the first occurrence of an unshadowed reference to the `needle`
/// encountered in an in-order traversal of the expression. This will generally
/// be the earliest occurrence within the expression string, but it isn't
/// guaranteed.
///
/// This doesn't need to use an environment. Instead, it just skips processing
/// any sub-expressions in which `needle` would be shadowed.
pub(super) fn first_reference<'a>(
    expr: &'a ResolvedExpr,
    needle: &str,
) -> Option<&'a Located<String>> {
    match expr.op() {
        ExprOp::Literal(_) => None,
        ExprOp::Reference(ident) => {
            if ident.inner() == needle {
                Some(ident)
            } else {
                None
            }
        }
        ExprOp::FieldRef(_, _) => recurse(expr.args(), needle),
        ExprOp::Call(_) => recurse(expr.args(), needle),
        ExprOp::Pipe(_) => {
            let lhs = first_reference(expr.args()[0].inner(), needle);
            if lhs.is_some() {
                lhs
            } else if needle == "$input" {
                // The RHS of the pipe would shadow the needle, so we won't find a free
                // reference there.
                None
            } else {
                first_reference(expr.args()[1].inner(), needle)
            }
        }
        ExprOp::Let(names, _) => {
            let bindings = names.len() - 1;
            for (name, value) in izip!(names, expr.args().iter().take(bindings)) {
                if name.inner() == needle {
                    // Once we've shadowed the needle, no later bindings nor the body could
                    // be a free reference to it.
                    return None;
                }

                let found = first_reference(value.inner(), needle);
                if found.is_some() {
                    return found;
                }
            }

            first_reference(expr.args()[bindings].inner(), needle)
        }
        ExprOp::Record(_, _) => recurse(expr.args(), needle),
        ExprOp::ExtendRecord(_) | ExprOp::RemoveFields(_) | ExprOp::SelectFields(_) => {
            recurse(expr.args(), needle)
        }
        ExprOp::Cast(_, _) => recurse(expr.args(), needle),
        ExprOp::Error => None,
    }
}

fn recurse<'a>(
    args: &'a Resolved<Located<Box<ResolvedExpr>>>,
    needle: &str,
) -> Option<&'a Located<String>> {
    args.iter().find_map(|arg| {
        let arg = arg.inner();
        first_reference(arg.as_ref(), needle)
    })
}

#[cfg(test)]
mod tests {
    use std::ops::Range;

    use sparrow_syntax::{Expr, FeatureSetPart};

    use super::*;
    use crate::frontend::resolve_arguments::resolve_recursive;

    fn test_first_reference(input: &'static str, needle: &'static str) -> Option<Range<usize>> {
        let mut diagnostics = Vec::new();
        let part_id = FeatureSetPart::Internal(input);
        let expr = Expr::try_from_str(part_id, input).unwrap();
        let expr = resolve_recursive(&expr, &mut diagnostics).unwrap();

        first_reference(&expr, needle).map(|found| found.location().start()..found.location().end())
    }

    #[test]
    fn test_first_reference_basic() {
        assert_eq!(test_first_reference("Foo + Bar.x", "Foo"), Some(0..3));
        assert_eq!(test_first_reference("Foo + Bar.x", "Bar"), Some(6..9));
        assert_eq!(test_first_reference("Foo + Bar.x", "x"), None);

        assert_eq!(test_first_reference("Foo.x + Foo.y", "Foo"), Some(0..3));
    }

    #[test]
    fn test_first_reference_pipe() {
        assert_eq!(
            test_first_reference("Foo.x | ($input + Bar)", "Foo"),
            Some(0..3)
        );
        assert_eq!(
            test_first_reference("Foo.x | ($input + Bar)", "Bar"),
            Some(18..21)
        );
        assert_eq!(
            test_first_reference("Foo.x | ($input + Bar)", "$input"),
            None
        );
    }

    #[test]
    fn test_first_reference_let() {
        // No unbound reference since it is immediately shadowed.
        assert_eq!(
            test_first_reference("let Foo = { x: 8 } in Foo.x + Bar.y", "Foo"),
            None
        );

        // Bar isn't shadowed.
        assert_eq!(
            test_first_reference("let Foo = { x: 8 } in Foo.x + Bar.y", "Bar"),
            Some(30..33)
        );

        // Bar is shadowed by the second binding.
        assert_eq!(
            test_first_reference("let Foo = { x: 8 } let Bar = { x: 9 } in Bar.y", "Bar"),
            None
        );

        // Bar is not shadowed in the binding of `Foo`.
        assert_eq!(
            test_first_reference("let Foo = Bar.x let Bar = { x: 9 } in Bar.y", "Bar"),
            Some(10..13)
        );
    }
}
