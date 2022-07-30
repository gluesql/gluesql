use {
    super::ExprNode,
    crate::{
        ast::{Expr, OrderByExpr},
        parse_sql::parse_order_by_expr,
        result::{Error, Result},
        translate::translate_order_by_expr,
    },
};

#[derive(Clone)]
pub enum OrderByExprNode {
    Expr(ExprNode, Option<bool>),
    Text(String),
}

impl From<(ExprNode, Option<bool>)> for OrderByExprNode {
    fn from((node, asc): (ExprNode, Option<bool>)) -> Self {
        Self::Expr(node, asc)
    }
}

impl From<&str> for OrderByExprNode {
    fn from(expr: &str) -> Self {
        Self::Text(expr.to_owned())
    }
}

impl TryFrom<OrderByExprNode> for OrderByExpr {
    type Error = Error;

    fn try_from(node: OrderByExprNode) -> Result<Self> {
        match node {
            OrderByExprNode::Expr(node, asc) => Ok(OrderByExpr {
                expr: Expr::try_from(node)?,
                asc,
            }),
            OrderByExprNode::Text(expr) => {
                let expr = parse_order_by_expr(expr).and_then(|op| translate_order_by_expr(&op))?;
                Ok(expr)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        ast_builder::{col, OrderByExprNode},
        parse_sql::parse_order_by_expr,
        translate::translate_order_by_expr,
    };

    fn test(actual: OrderByExprNode, expected: &str) {
        let parsed = &parse_order_by_expr(expected).unwrap();
        let expected = translate_order_by_expr(parsed);
        assert_eq!(actual.try_into(), expected);
    }

    #[test]
    fn select_item() {
        let actual = OrderByExprNode::Expr(col("foo"), None);
        let expected = "foo";
        test(actual, expected);

        let actual = OrderByExprNode::Expr(col("foo"), Some(true));
        let expected = "foo ASC";
        test(actual, expected);

        let actual = OrderByExprNode::Expr(col("foo"), Some(false));
        let expected = "foo DESC";
        test(actual, expected);

        let actual = OrderByExprNode::Text("foo asc".to_string());
        let expected = "foo ASC";
        test(actual, expected);

        let actual = OrderByExprNode::Text("foo desc".to_string());
        let expected = "foo DESC";
        test(actual, expected);
    }
}
