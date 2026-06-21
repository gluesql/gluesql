use {
    super::{ExprNode, OrderByExprNode},
    crate::{
        ast::OrderByExpr,
        parse_sql::parse_order_by_exprs,
        plan::OrderByExprPlan,
        result::Result,
        translate::{NO_PARAMS, translate_order_by_expr},
    },
};

#[derive(Clone, Debug)]
pub enum OrderByExprList<'a> {
    Text(String),
    OrderByExprs(Vec<OrderByExprNode<'a>>),
}

impl From<&str> for OrderByExprList<'_> {
    fn from(exprs: &str) -> Self {
        OrderByExprList::Text(exprs.to_owned())
    }
}

impl From<Vec<&str>> for OrderByExprList<'_> {
    fn from(exprs: Vec<&str>) -> Self {
        OrderByExprList::OrderByExprs(exprs.into_iter().map(Into::into).collect())
    }
}

impl<'a> From<OrderByExprNode<'a>> for OrderByExprList<'a> {
    fn from(expr: OrderByExprNode<'a>) -> Self {
        OrderByExprList::OrderByExprs(vec![expr])
    }
}

impl<'a> From<Vec<OrderByExprNode<'a>>> for OrderByExprList<'a> {
    fn from(exprs: Vec<OrderByExprNode<'a>>) -> Self {
        OrderByExprList::OrderByExprs(exprs)
    }
}

impl<'a> From<ExprNode<'a>> for OrderByExprList<'a> {
    fn from(expr_node: ExprNode<'a>) -> Self {
        OrderByExprList::OrderByExprs(vec![expr_node.into()])
    }
}

impl OrderByExprList<'_> {
    pub(super) fn build_order_by_exprs_plan(self) -> Result<Vec<OrderByExprPlan>> {
        match self {
            OrderByExprList::Text(exprs) => parse_order_by_exprs(exprs)?
                .iter()
                .map(|expr| translate_order_by_expr(expr, NO_PARAMS).map(Into::into))
                .collect::<Result<Vec<_>>>(),
            OrderByExprList::OrderByExprs(exprs) => exprs
                .into_iter()
                .map(OrderByExprNode::build_order_by_expr_plan)
                .collect::<Result<Vec<_>>>(),
        }
    }

    pub(super) fn build_order_by_exprs(self) -> Result<Vec<OrderByExpr>> {
        match self {
            OrderByExprList::Text(exprs) => parse_order_by_exprs(exprs)?
                .iter()
                .map(|expr| translate_order_by_expr(expr, NO_PARAMS))
                .collect::<Result<Vec<_>>>(),
            OrderByExprList::OrderByExprs(exprs) => exprs
                .into_iter()
                .map(OrderByExprNode::build_order_by_expr)
                .collect::<Result<Vec<_>>>(),
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        crate::{
            ast::OrderByExpr,
            parse_sql::parse_order_by_exprs,
            query_builder::{OrderByExprList, col},
            result::Result,
            translate::{NO_PARAMS, translate_order_by_expr},
        },
        pretty_assertions::assert_eq,
    };

    fn expected(exprs: &str) -> Result<Vec<OrderByExpr>> {
        parse_order_by_exprs(exprs)?
            .iter()
            .map(|expr| translate_order_by_expr(expr, NO_PARAMS))
            .collect::<Result<Vec<_>>>()
    }

    #[test]
    fn order_by_expr_list() {
        let actual = OrderByExprList::from(col("foo"));
        assert_eq!(actual.build_order_by_exprs(), expected("foo"));

        let actual = OrderByExprList::from(col("foo").desc());
        assert_eq!(actual.build_order_by_exprs(), expected("foo DESC"));

        let actual = OrderByExprList::from(vec![col("foo").desc(), col("bar").asc()]);
        assert_eq!(actual.build_order_by_exprs(), expected("foo DESC, bar ASC"));
    }
}
