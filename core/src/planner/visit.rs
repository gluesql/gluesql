mod expr;
mod query;
mod source;

pub use {
    expr::{try_visit_expr, walk_expr},
    query::walk_query,
    source::walk_source,
};

use crate::plan::{ExprPlan, QueryPlan, SourcePlan};

// Override a visit method to apply a policy before or after walking its children.
// Walkers follow plan fields, never references into StatementPlan::functions.
pub trait Visitor {
    type Error;

    fn visit_expr(&mut self, expr: &ExprPlan) -> Result<(), Self::Error> {
        walk_expr(self, expr)
    }

    fn visit_query(&mut self, query: &QueryPlan) -> Result<(), Self::Error> {
        walk_query(self, query)
    }

    fn visit_source(&mut self, source: &SourcePlan) -> Result<(), Self::Error> {
        walk_source(self, source)
    }
}
