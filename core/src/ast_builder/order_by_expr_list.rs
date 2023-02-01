use {
    super::{ExprNode, OrderByExprNode},
    crate::{
        ast::OrderByExpr,
        parse_sql::parse_order_by_exprs,
        result::{Error, Result},
        translate::translate_order_by_expr,
    },
};

#[derive(Clone, Debug)]
pub enum OrderByExprList<'a> {
    Text(String),
    OrderByExprs(Vec<OrderByExprNode<'a>>),
}

impl<'a> From<&str> for OrderByExprList<'a> {
    fn from(exprs: &str) -> Self {
        OrderByExprList::Text(exprs.to_owned())
    }
}

impl<'a> From<Vec<&str>> for OrderByExprList<'a> {
    fn from(exprs: Vec<&str>) -> Self {
        OrderByExprList::OrderByExprs(exprs.into_iter().map(Into::into).collect())
    }
}

impl<'a> From<ExprNode<'a>> for OrderByExprList<'a> {
    fn from(expr_node: ExprNode<'a>) -> Self {
        OrderByExprList::OrderByExprs(vec![expr_node.into()])
    }
}

impl<'a> TryFrom<OrderByExprList<'a>> for Vec<OrderByExpr> {
    type Error = Error;

    fn try_from(order_by_exprs: OrderByExprList<'a>) -> Result<Self> {
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
