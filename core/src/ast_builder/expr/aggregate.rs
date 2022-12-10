use {
    super::ExprNode,
    crate::{
        ast::{Aggregate, CountArgExpr},
        parse_sql::parse_expr,
        result::{Error, Result},
        translate::translate_expr,
    },
};

#[derive(Clone, Debug)]
pub enum AggregateNode<'a> {
    Count(CountArgExprNode<'a>),
    Sum(ExprNode<'a>),
    Min(ExprNode<'a>),
    Max(ExprNode<'a>),
    Avg(ExprNode<'a>),
    Variance(ExprNode<'a>),
    Stdev(ExprNode<'a>),
}

#[derive(Clone, Debug)]
pub enum CountArgExprNode<'a> {
    Text(String),
    Expr(ExprNode<'a>),
}

impl<'a> From<&'a str> for CountArgExprNode<'a> {
    fn from(count_arg_str: &str) -> Self {
        Self::Text(count_arg_str.to_owned())
    }
}

impl<'a> From<ExprNode<'a>> for CountArgExprNode<'a> {
    fn from(expr_node: ExprNode<'a>) -> Self {
        Self::Expr(expr_node)
    }
}

impl<'a> TryFrom<CountArgExprNode<'a>> for CountArgExpr {
    type Error = Error;

    fn try_from(count_expr_node: CountArgExprNode<'a>) -> Result<Self> {
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

impl<'a> TryFrom<AggregateNode<'a>> for Aggregate {
    type Error = Error;

    fn try_from(aggr_node: AggregateNode<'a>) -> Result<Self> {
        match aggr_node {
            AggregateNode::Count(count_arg_expr_node) => {
                count_arg_expr_node.try_into().map(Aggregate::Count)
            }
            AggregateNode::Sum(expr_node) => expr_node.try_into().map(Aggregate::Sum),
            AggregateNode::Min(expr_node) => expr_node.try_into().map(Aggregate::Min),
            AggregateNode::Max(expr_node) => expr_node.try_into().map(Aggregate::Max),
            AggregateNode::Avg(expr_node) => expr_node.try_into().map(Aggregate::Avg),
            AggregateNode::Variance(expr_node) => expr_node.try_into().map(Aggregate::Variance),
            AggregateNode::Stdev(expr_node) => expr_node.try_into().map(Aggregate::Stdev),
        }
    }
}

impl<'a> ExprNode<'a> {
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

    pub fn stdev(self) -> Self {
        stdev(self)
    }
}

pub fn count<'a, T: Into<CountArgExprNode<'a>>>(expr: T) -> ExprNode<'a> {
    ExprNode::Aggregate(Box::new(AggregateNode::Count(expr.into())))
}

pub fn sum<'a, T: Into<ExprNode<'a>>>(expr: T) -> ExprNode<'a> {
    ExprNode::Aggregate(Box::new(AggregateNode::Sum(expr.into())))
}

pub fn min<'a, T: Into<ExprNode<'a>>>(expr: T) -> ExprNode<'a> {
    ExprNode::Aggregate(Box::new(AggregateNode::Min(expr.into())))
}

pub fn max<'a, T: Into<ExprNode<'a>>>(expr: T) -> ExprNode<'a> {
    ExprNode::Aggregate(Box::new(AggregateNode::Max(expr.into())))
}

pub fn avg<'a, T: Into<ExprNode<'a>>>(expr: T) -> ExprNode<'a> {
    ExprNode::Aggregate(Box::new(AggregateNode::Avg(expr.into())))
}

pub fn variance<'a, T: Into<ExprNode<'a>>>(expr: T) -> ExprNode<'a> {
    ExprNode::Aggregate(Box::new(AggregateNode::Variance(expr.into())))
}

pub fn stdev<'a, T: Into<ExprNode<'a>>>(expr: T) -> ExprNode<'a> {
    ExprNode::Aggregate(Box::new(AggregateNode::Stdev(expr.into())))
}

#[cfg(test)]
mod tests {
    use crate::ast_builder::{avg, col, count, max, min, stdev, sum, test_expr, variance};

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

        let actual = col("scatterplot").stdev();
        let expected = "STDEV(scatterplot)";
        test_expr(actual, expected);

        let actual = stdev("scatterplot");
        let expected = "STDEV(scatterplot)";
        test_expr(actual, expected);
    }
}
