mod expr;
mod query;
mod source;

pub use {
    expr::{visit_mut_expr, walk_expr_mut},
    query::walk_query_mut,
    source::walk_source_mut,
};

use crate::plan::{ExprPlan, QueryPlan, SourcePlan};

// Override a visit method to apply a policy before or after walking its children.
// Walkers follow plan fields, never references into StatementPlan::functions.
pub trait VisitorMut {
    type Error;

    fn visit_expr(&mut self, expr: &mut ExprPlan) -> Result<(), Self::Error> {
        walk_expr_mut(self, expr)
    }

    fn visit_query(&mut self, query: &mut QueryPlan) -> Result<(), Self::Error> {
        walk_query_mut(self, query)
    }

    fn visit_source(&mut self, source: &mut SourcePlan) -> Result<(), Self::Error> {
        walk_source_mut(self, source)
    }
}
