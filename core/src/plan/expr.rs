mod plan_expr;
mod visit;

pub mod deterministic;
pub mod evaluable;
pub mod nullability;
pub use plan_expr::PlanExpr;
pub use visit::{try_visit_expr, visit_mut_expr};

use super::ExprPlan;

pub fn plan_scalar_expr(expr: crate::ast::Expr) -> ExprPlan {
    expr.into()
}
