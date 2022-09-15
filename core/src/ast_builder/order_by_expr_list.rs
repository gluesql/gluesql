use {
    super::{ExprNode, OrderByExprNode},
    crate::{
        ast::OrderByExpr,
        parse_sql::parse_order_by_exprs,
        result::{Error, Result},
        translate::translate_order_by_expr,
    },
};

#[derive(Clone)]
pub enum OrderByExprList {
    Text(String),
    OrderByExprs(Vec<OrderByExprNode>),
}

impl From<&str> for OrderByExprList {
    fn from(exprs: &str) -> Self {
        OrderByExprList::Text(exprs.to_owned())
    }
}

impl From<Vec<&str>> for OrderByExprList {
    fn from(exprs: Vec<&str>) -> Self {
        OrderByExprList::OrderByExprs(exprs.into_iter().map(Into::into).collect())
    }
}

impl From<ExprNode> for OrderByExprList {
    fn from(expr_node: ExprNode) -> Self {
        OrderByExprList::OrderByExprs(vec![expr_node.into()])
    }
}

impl TryFrom<OrderByExprList> for Vec<OrderByExpr> {
    type Error = Error;

    fn try_from(order_by_exprs: OrderByExprList) -> Result<Self> {
        match order_by_exprs {
            OrderByExprList::Text(exprs) => parse_order_by_exprs(exprs)?
                .iter()
                .map(translate_order_by_expr)
                .collect::<Result<Vec<_>>>(),
            OrderByExprList::OrderByExprs(exprs) => exprs
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<Vec<_>>>(),
        }
    }
}
