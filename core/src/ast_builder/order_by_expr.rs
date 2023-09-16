use {
    super::ExprNode,
    crate::{
        ast::{Expr, OrderByExpr},
        parse_sql::parse_order_by_expr,
        result::{Error, Result},
        translate::translate_order_by_expr,
    },
};

#[derive(Clone, Debug)]
pub enum OrderByExprNode<'a> {
    Text(String),
    Expr(ExprNode<'a>),
}

impl<'a> From<&str> for OrderByExprNode<'a> {
    fn from(expr: &str) -> Self {
        Self::Text(expr.to_owned())
    }
}

impl<'a> From<ExprNode<'a>> for OrderByExprNode<'a> {
    fn from(expr_node: ExprNode<'a>) -> Self {
        Self::Expr(expr_node)
    }
}

impl<'a> TryFrom<OrderByExprNode<'a>> for OrderByExpr {
    type Error = Error;

    fn try_from(node: OrderByExprNode<'a>) -> Result<Self> {
        match node {
            OrderByExprNode::Text(expr) => {
                let expr = parse_order_by_expr(expr).and_then(|op| translate_order_by_expr(&op))?;
                Ok(expr)
            }
            OrderByExprNode::Expr(expr_node) => {
                let expr = Expr::try_from(expr_node)?;

                Ok(OrderByExpr { expr, asc: None })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        crate::{
            ast_builder::OrderByExprNode, parse_sql::parse_order_by_expr,
            translate::translate_order_by_expr,
        },
        pretty_assertions::assert_eq,
    };

    fn test(actual: OrderByExprNode, expected: &str) {
        let parsed = &parse_order_by_expr(expected).expect(expected);
        let expected = translate_order_by_expr(parsed);
        assert_eq!(actual.try_into(), expected);
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
    }
}
