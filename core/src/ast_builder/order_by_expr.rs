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
    OrderByExpr(OrderByExpr),
    Expr(ExprNode),
    Text(String),
}

impl From<OrderByExpr> for OrderByExprNode {
    fn from(order_item: OrderByExpr) -> Self {
        Self::OrderByExpr(order_item)
    }
}

impl From<ExprNode> for OrderByExprNode {
    fn from(expr_node: ExprNode) -> Self {
        Self::Expr(expr_node)
    }
}

impl From<&str> for OrderByExprNode {
    fn from(select_item: &str) -> Self {
        Self::Text(select_item.to_owned())
    }
}

impl TryFrom<OrderByExprNode> for OrderByExpr {
    type Error = Error;

    fn try_from(order_by_expr_node: OrderByExprNode) -> Result<Self> {
        match order_by_expr_node {
            OrderByExprNode::OrderByExpr(order_by_expr) => Ok(order_by_expr),
            OrderByExprNode::Text(order_by_expr) => {
                parse_order_by_expr(order_by_expr).and_then(|op| translate_order_by_expr(&op))
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
    use crate::{
        ast_builder::OrderByExprNode, parse_sql::parse_order_by_expr,
        translate::translate_order_by_expr,
    };

    fn test(actual: OrderByExprNode, expected: &str) {
        let parsed = &parse_order_by_expr(expected).unwrap();
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
