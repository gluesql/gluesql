use {
    super::ExprNode,
    crate::{
        ast::OrderByExpr,
        parse_sql::parse_order_by_expr,
        plan::OrderByExprPlan,
        result::Result,
        translate::{NO_PARAMS, translate_order_by_expr},
    },
};

#[derive(Clone, Debug)]
pub enum OrderByExprNode<'a> {
    Text(String),
    Expr {
        expr: ExprNode<'a>,
        asc: Option<bool>,
    },
}

impl From<&str> for OrderByExprNode<'_> {
    fn from(expr: &str) -> Self {
        Self::Text(expr.to_owned())
    }
}

impl<'a> From<ExprNode<'a>> for OrderByExprNode<'a> {
    fn from(expr_node: ExprNode<'a>) -> Self {
        Self::Expr {
            expr: expr_node,
            asc: None,
        }
    }
}

impl OrderByExprNode<'_> {
    pub(super) fn build_order_by_expr(self) -> Result<OrderByExpr> {
        match self {
            OrderByExprNode::Text(expr) => {
                parse_order_by_expr(expr).and_then(|op| translate_order_by_expr(&op, NO_PARAMS))
            }
            OrderByExprNode::Expr { expr, asc } => {
                let expr = expr.build_expr()?;

                Ok(OrderByExpr { expr, asc })
            }
        }
    }

    pub(super) fn build_order_by_expr_plan(self) -> Result<OrderByExprPlan> {
        match self {
            OrderByExprNode::Text(expr) => parse_order_by_expr(expr)
                .and_then(|op| translate_order_by_expr(&op, NO_PARAMS).map(Into::into)),
            OrderByExprNode::Expr { expr, asc } => {
                let expr = expr.build_expr_plan()?;

                Ok(OrderByExprPlan { expr, asc })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        crate::{
            ast_builder::{OrderByExprNode, col},
            parse_sql::parse_order_by_expr,
            plan::OrderByExprPlan,
            translate::{NO_PARAMS, translate_order_by_expr},
        },
        pretty_assertions::assert_eq,
    };

    fn test(actual: OrderByExprNode, expected: &str) {
        let parsed = &parse_order_by_expr(expected).expect(expected);
        let expected = translate_order_by_expr(parsed, NO_PARAMS).map(OrderByExprPlan::from);
        assert_eq!(actual.build_order_by_expr_plan(), expected);
    }

    #[test]
    fn order_by_expr() {
        let actual = OrderByExprNode::Text("foo".into());
        let expected = "foo";
        test(actual, expected);

        let actual = OrderByExprNode::Text("foo asc".into());
        let expected = "foo ASC";
        test(actual, expected);

        let actual = OrderByExprNode::Text("foo desc".into());
        let expected = "foo DESC";
        test(actual, expected);

        let actual = OrderByExprNode::from(col("foo"));
        let expected = "foo";
        test(actual, expected);

        let actual = col("foo").asc();
        let expected = "foo ASC";
        test(actual, expected);

        let actual = col("foo").desc();
        let expected = "foo DESC";
        test(actual, expected);
    }
}
