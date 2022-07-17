use {
    super::ExprNode,
    crate::{
        ast::{Aggregate, CountArgExpr, Expr},
        parse_sql::parse_expr,
        result::{Error, Result},
        translate::translate_expr,
    },
};

#[derive(Clone)]
pub enum AggregateNode {
    Count(CountArgExprNode),
    Sum(ExprNode),
    Min(ExprNode),
    Max(ExprNode),
    Avg(ExprNode),
    Variance(ExprNode),
}

#[derive(Clone)]
pub enum CountArgExprNode {
    Text(String),
    Expr(ExprNode),
}

impl From<&str> for CountArgExprNode {
    fn from(count_arg_str: &str) -> Self {
        Self::Text(count_arg_str.to_owned())
    }
}

impl From<ExprNode> for CountArgExprNode {
    fn from(expr_node: ExprNode) -> Self {
        Self::Expr(expr_node)
    }
}

impl TryFrom<CountArgExprNode> for CountArgExpr {
    type Error = Error;

    fn try_from(count_expr_node: CountArgExprNode) -> Result<Self> {
        match count_expr_node {
            CountArgExprNode::Text(s) if &s == "*" => Ok(CountArgExpr::Wildcard),
            CountArgExprNode::Text(s) => {
                let expr = parse_expr(s).and_then(|expr| translate_expr(&expr))?;

                Ok(CountArgExpr::Expr(expr))
            }
            CountArgExprNode::Expr(expr_node) => expr_node.try_into().map(CountArgExpr::Expr),
        }
    }
}

impl TryFrom<AggregateNode> for Expr {
    type Error = Error;

    fn try_from(aggr_node: AggregateNode) -> Result<Self> {
        match aggr_node {
            AggregateNode::Count(count_arg_expr_node) => count_arg_expr_node
                .try_into()
                .map(Aggregate::Count)
                .map(Box::new)
                .map(Expr::Aggregate),
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
    pub fn count(self) -> Self {
        count(self)
    }

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

pub fn count<T: Into<CountArgExprNode>>(expr: T) -> ExprNode {
    ExprNode::Aggregate(Box::new(AggregateNode::Count(expr.into())))
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
    use crate::ast_builder::{avg, col, count, max, min, sum, test_expr, variance};

    #[test]
    fn aggregate() {
        let actual = col("id").count();
        let expected = "COUNT(id)";
        test_expr(actual, expected);

        let actual = count("id");
        let expected = "COUNT(id)";
        test_expr(actual, expected);

        let actual = count("*");
        let expected = "COUNT(*)";
        test_expr(actual, expected);

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
