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
    Min(ExprNode),
    Max(ExprNode),
    Avg(ExprNode),
    Variance(ExprNode),
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
            AggregateNode::Min(expr_node) => expr_node
                .try_into()
                .map(Aggregate::Min)
                .map(Box::new)
                .map(Expr::Aggregate),
            AggregateNode::Max(expr_node) => expr_node
                .try_into()
                .map(Aggregate::Max)
                .map(Box::new)
                .map(Expr::Aggregate),
            AggregateNode::Avg(expr_node) => expr_node
                .try_into()
                .map(Aggregate::Avg)
                .map(Box::new)
                .map(Expr::Aggregate),
            AggregateNode::Variance(expr_node) => expr_node
                .try_into()
                .map(Aggregate::Variance)
                .map(Box::new)
                .map(Expr::Aggregate),
        }
    }
}

impl ExprNode {
    pub fn sum(self) -> Self {
        sum(self)
    }

    pub fn min(self) -> Self {
        min(self)
    }

    pub fn max(self) -> Self {
        max(self)
    }

    pub fn avg(self) -> Self {
        avg(self)
    }

    pub fn variance(self) -> Self {
        variance(self)
    }
}

pub fn sum<T: Into<ExprNode>>(expr: T) -> ExprNode {
    ExprNode::Aggregate(Box::new(AggregateNode::Sum(expr.into())))
}

pub fn min<T: Into<ExprNode>>(expr: T) -> ExprNode {
    ExprNode::Aggregate(Box::new(AggregateNode::Min(expr.into())))
}

pub fn max<T: Into<ExprNode>>(expr: T) -> ExprNode {
    ExprNode::Aggregate(Box::new(AggregateNode::Max(expr.into())))
}

pub fn avg<T: Into<ExprNode>>(expr: T) -> ExprNode {
    ExprNode::Aggregate(Box::new(AggregateNode::Avg(expr.into())))
}

pub fn variance<T: Into<ExprNode>>(expr: T) -> ExprNode {
    ExprNode::Aggregate(Box::new(AggregateNode::Variance(expr.into())))
}

#[cfg(test)]
mod tests {
    use crate::ast_builder::{avg, col, max, min, sum, test_expr, variance};

    #[test]
    fn aggregate() {
        let actual = col("amount").sum();
        let expected = "SUM(amount)";
        test_expr(actual, expected);

        let actual = sum("amount");
        let expected = "SUM(amount)";
        test_expr(actual, expected);

        let actual = col("budget").min();
        let expected = "MIN(budget)";
        test_expr(actual, expected);
        let actual = min("budget");
        let expected = "MIN(budget)";
        test_expr(actual, expected);

        let actual = col("score").max();
        let expected = "MAX(score)";
        test_expr(actual, expected);

        let actual = max("score");
        let expected = "MAX(score)";
        test_expr(actual, expected);

        let actual = col("grade").avg();
        let expected = "AVG(grade)";
        test_expr(actual, expected);

        let actual = avg("grade");
        let expected = "AVG(grade)";
        test_expr(actual, expected);

        let actual = col("statistic").variance();
        let expected = "VARIANCE(statistic)";
        test_expr(actual, expected);

        let actual = variance("statistic");
        let expected = "VARIANCE(statistic)";
        test_expr(actual, expected);
    }
}
