use std::str::FromStr;

use crate::dfg::language::DfgLang;

#[derive(Debug)]
pub struct DfgPattern(egg::Pattern<DfgLang>);

impl FromStr for DfgPattern {
    type Err = anyhow::Error;

    fn from_str(pattern: &str) -> Result<Self, Self::Err> {
        let pattern = egg::Pattern::from_str(pattern)
            .map_err(|_| anyhow::anyhow!("Unable to parse pattern '{}'", pattern))?;

        Ok(Self(pattern))
    }
}

impl DfgPattern {
    pub fn references(&self, var: &egg::Var) -> bool {
        self.0.vars().contains(var)
    }

    pub(super) fn ast(&self) -> &egg::RecExpr<egg::ENodeOrVar<DfgLang>> {
        &self.0.ast
    }
}
