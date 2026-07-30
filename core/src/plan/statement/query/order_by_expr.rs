use {
    crate::{ast, plan::ExprPlan},
    serde::{Deserialize, Serialize},
};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct OrderByExprPlan {
    pub expr: ExprPlan,
    pub asc: Option<bool>,
}

impl From<ast::OrderByExpr> for OrderByExprPlan {
    fn from(order_by_expr: ast::OrderByExpr) -> Self {
        let ast::OrderByExpr { expr, asc } = order_by_expr;

        Self {
            expr: expr.into(),
            asc,
        }
    }
}
