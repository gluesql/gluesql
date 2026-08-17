use {
    crate::{
        ast,
        plan::{
            ExprPlan,
            explain::{Explain, ExplainContext},
        },
    },
    serde::{Deserialize, Serialize},
};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct OrderByExprPlan {
    pub expr: ExprPlan,
    pub asc: Option<bool>,
}

impl Explain for OrderByExprPlan {
    type Output = String;

    fn explain(&self, context: &mut ExplainContext) -> String {
        let mut output = self.expr.explain(context);
        if let Some(asc) = self.asc {
            output.push_str(if asc { " ASC" } else { " DESC" });
        }
        output
    }
}

impl Explain for [OrderByExprPlan] {
    type Output = String;

    fn explain(&self, context: &mut ExplainContext) -> String {
        let mut output = String::new();
        for (index, order_by) in self.iter().enumerate() {
            if index > 0 {
                output.push_str(", ");
            }
            output.push_str(&order_by.explain(context));
        }
        output
    }
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

#[cfg(test)]
mod tests {
    use {
        super::OrderByExprPlan,
        crate::plan::{
            ExprPlan,
            explain::{Explain, ExplainContext},
        },
    };

    #[test]
    fn explain() {
        let actual = [
            OrderByExprPlan {
                expr: ExprPlan::Identifier("name".to_owned()),
                asc: Some(true),
            },
            OrderByExprPlan {
                expr: ExprPlan::Identifier("created_at".to_owned()),
                asc: Some(false),
            },
            OrderByExprPlan {
                expr: ExprPlan::Identifier("id".to_owned()),
                asc: None,
            },
        ];
        let expected = "name ASC, created_at DESC, id";

        assert_eq!(
            actual.as_slice().explain(&mut ExplainContext::default()),
            expected
        );
    }
}
