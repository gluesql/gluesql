use {
    super::ExprNode,
    crate::{
        ast::{Aggregate, Expr},
        result::{Error, Result},
    },
};

#[derive(Clone)]
pub enum AggregateNode {
    Sum(ExprNode),
    Max(ExprNode),
}

impl TryFrom<AggregateNode> for Expr {
    type Error = Error;

    fn try_from(aggr_node: AggregateNode) -> Result<Self> {
        match aggr_node {
            AggregateNode::Sum(expr_node) => expr_node
                .try_into()
                .map(Aggregate::Sum)
                .map(Box::new)
                .map(Expr::Aggregate),
            AggregateNode::Max(expr_node) => expr_node
                .try_into()
                .map(Aggregate::Max)
                .map(Box::new)
                .map(Expr::Aggregate),
        }
    }
}

impl ExprNode {
    pub fn sum(self) -> Self {
        sum(self)
    }

    pub fn max(self) -> Self {
        max(self)
    }
}

pub fn sum<T: Into<ExprNode>>(expr: T) -> ExprNode {
    ExprNode::Aggregate(Box::new(AggregateNode::Sum(expr.into())))
}

pub fn max<T: Into<ExprNode>>(expr: T) -> ExprNode {
    ExprNode::Aggregate(Box::new(AggregateNode::Max(expr.into())))
}

#[cfg(test)]
mod tests {
    use crate::ast_builder::{col, max, sum, test_expr};

    #[test]
    fn aggregate() {
        let actual = col("amount").sum();
        let expected = "SUM(amount)";
        test_expr(actual, expected);

        let actual = sum("amount");
        let expected = "SUM(amount)";
        test_expr(actual, expected);

        let actual = col("score").max();
        let expected = "MAX(score)";
        test_expr(actual, expected);

        let actual = max("score");
        let expected = "MAX(score)";
        test_expr(actual, expected);
    }
}
