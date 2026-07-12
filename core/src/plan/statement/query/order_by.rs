use {
    super::SetExprPlan,
    crate::{ast, plan::ExprPlan},
    serde::{Deserialize, Serialize},
};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct OrderByPlan {
    pub input: SetExprPlan,
    pub exprs: Vec<OrderByExprPlan>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct OrderByExprPlan {
    pub expr: ExprPlan,
    pub asc: Option<bool>,
}

impl From<ast::OrderByExpr> for OrderByExprPlan {
    fn from(order_by: ast::OrderByExpr) -> Self {
        let ast::OrderByExpr { expr, asc } = order_by;

        Self {
            expr: expr.into(),
            asc,
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::{OrderByExprPlan, OrderByPlan},
        crate::{
            ast::Literal,
            plan::{ExprPlan, SetExprPlan, ValuesPlan},
        },
    };

    #[test]
    fn order_by_accepts_set_expr_input() {
        let plan = OrderByPlan {
            input: SetExprPlan::Values(ValuesPlan(Vec::new())),
            exprs: vec![OrderByExprPlan {
                expr: ExprPlan::Literal(Literal::Number(1.into())),
                asc: Some(false),
            }],
        };

        assert!(matches!(plan.input, SetExprPlan::Values(_)));
        assert_eq!(plan.exprs.len(), 1);
    }
}
