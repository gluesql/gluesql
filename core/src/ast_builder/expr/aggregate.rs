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
    Count(CountArgExprNode<'a>, bool), // second field is distinct
    Sum(ExprNode<'a>, bool),
    Total(ExprNode<'a>, bool),
    Min(ExprNode<'a>, bool),
    Max(ExprNode<'a>, bool),
    Avg(ExprNode<'a>, bool),
    Variance(ExprNode<'a>, bool),
    Stdev(ExprNode<'a>, bool),
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
            AggregateNode::Count(count_arg_expr_node, distinct) => count_arg_expr_node
                .try_into()
                .map(|expr| Aggregate::count(expr, distinct)),
            AggregateNode::Sum(expr_node, distinct) => expr_node
                .try_into()
                .map(|expr| Aggregate::sum(expr, distinct)),
            AggregateNode::Total(expr_node, distinct) => expr_node
                .try_into()
                .map(|expr| Aggregate::total(expr, distinct)),
            AggregateNode::Min(expr_node, distinct) => expr_node
                .try_into()
                .map(|expr| Aggregate::min(expr, distinct)),
            AggregateNode::Max(expr_node, distinct) => expr_node
                .try_into()
                .map(|expr| Aggregate::max(expr, distinct)),
            AggregateNode::Avg(expr_node, distinct) => expr_node
                .try_into()
                .map(|expr| Aggregate::avg(expr, distinct)),
            AggregateNode::Variance(expr_node, distinct) => expr_node
                .try_into()
                .map(|expr| Aggregate::variance(expr, distinct)),
            AggregateNode::Stdev(expr_node, distinct) => expr_node
                .try_into()
                .map(|expr| Aggregate::stdev(expr, distinct)),
        }
    }
}

impl<'a> ExprNode<'a> {
    pub fn count(self) -> ExprNode<'a> {
        ExprNode::Aggregate(Box::new(AggregateNode::Count(self.into(), false)))
    }

    pub fn count_distinct(self) -> ExprNode<'a> {
        ExprNode::Aggregate(Box::new(AggregateNode::Count(self.into(), true)))
    }

    pub fn sum(self) -> ExprNode<'a> {
        ExprNode::Aggregate(Box::new(AggregateNode::Sum(self, false)))
    }

    pub fn sum_distinct(self) -> ExprNode<'a> {
        ExprNode::Aggregate(Box::new(AggregateNode::Sum(self, true)))
    }
  
    pub fn total(self) -> ExprNode<'a> {
        ExprNode::Aggregate(Box::new(AggregateNode::Total(self, false)))
    }

    pub fn min(self) -> ExprNode<'a> {
        ExprNode::Aggregate(Box::new(AggregateNode::Min(self, false)))
    }

    pub fn min_distinct(self) -> ExprNode<'a> {
        ExprNode::Aggregate(Box::new(AggregateNode::Min(self, true)))
    }

    pub fn max(self) -> ExprNode<'a> {
        ExprNode::Aggregate(Box::new(AggregateNode::Max(self, false)))
    }

    pub fn max_distinct(self) -> ExprNode<'a> {
        ExprNode::Aggregate(Box::new(AggregateNode::Max(self, true)))
    }

    pub fn avg(self) -> ExprNode<'a> {
        ExprNode::Aggregate(Box::new(AggregateNode::Avg(self, false)))
    }
    
    pub fn avg_distinct(self) -> ExprNode<'a> {
        ExprNode::Aggregate(Box::new(AggregateNode::Avg(self, true)))
    }

    pub fn variance(self) -> ExprNode<'a> {
        ExprNode::Aggregate(Box::new(AggregateNode::Variance(self, false)))
    }

    pub fn variance_distinct(self) -> ExprNode<'a> {
        ExprNode::Aggregate(Box::new(AggregateNode::Variance(self, true)))
    }

    pub fn stdev(self) -> ExprNode<'a> {
        ExprNode::Aggregate(Box::new(AggregateNode::Stdev(self, false)))
    }

    pub fn stdev_distinct(self) -> ExprNode<'a> {
        ExprNode::Aggregate(Box::new(AggregateNode::Stdev(self, true)))
    }
}

pub fn count<'a, T: Into<CountArgExprNode<'a>>>(expr: T) -> ExprNode<'a> {
    ExprNode::Aggregate(Box::new(AggregateNode::Count(expr.into(), false)))
}

pub fn count_distinct<'a, T: Into<CountArgExprNode<'a>>>(expr: T) -> ExprNode<'a> {
    ExprNode::Aggregate(Box::new(AggregateNode::Count(expr.into(), true)))
}

pub fn sum<'a, T: Into<ExprNode<'a>>>(expr: T) -> ExprNode<'a> {
    ExprNode::Aggregate(Box::new(AggregateNode::Sum(expr.into(), false)))
}

pub fn sum_distinct<'a, T: Into<ExprNode<'a>>>(expr: T) -> ExprNode<'a> {
    ExprNode::Aggregate(Box::new(AggregateNode::Sum(expr.into(), true)))
}

pub fn total<'a, T: Into<ExprNode<'a>>>(expr: T) -> ExprNode<'a> {
    ExprNode::Aggregate(Box::new(AggregateNode::Total(expr.into(), false)))
}

pub fn min<'a, T: Into<ExprNode<'a>>>(expr: T) -> ExprNode<'a> {
    ExprNode::Aggregate(Box::new(AggregateNode::Min(expr.into(), false)))
}

pub fn min_distinct<'a, T: Into<ExprNode<'a>>>(expr: T) -> ExprNode<'a> {
    ExprNode::Aggregate(Box::new(AggregateNode::Min(expr.into(), true)))
}

pub fn max<'a, T: Into<ExprNode<'a>>>(expr: T) -> ExprNode<'a> {
    ExprNode::Aggregate(Box::new(AggregateNode::Max(expr.into(), false)))
}

pub fn max_distinct<'a, T: Into<ExprNode<'a>>>(expr: T) -> ExprNode<'a> {
    ExprNode::Aggregate(Box::new(AggregateNode::Max(expr.into(), true)))
}

pub fn avg<'a, T: Into<ExprNode<'a>>>(expr: T) -> ExprNode<'a> {
    ExprNode::Aggregate(Box::new(AggregateNode::Avg(expr.into(), false)))
}

pub fn avg_distinct<'a, T: Into<ExprNode<'a>>>(expr: T) -> ExprNode<'a> {
    ExprNode::Aggregate(Box::new(AggregateNode::Avg(expr.into(), true)))
}

pub fn variance<'a, T: Into<ExprNode<'a>>>(expr: T) -> ExprNode<'a> {
    ExprNode::Aggregate(Box::new(AggregateNode::Variance(expr.into(), false)))
}

pub fn variance_distinct<'a, T: Into<ExprNode<'a>>>(expr: T) -> ExprNode<'a> {
    ExprNode::Aggregate(Box::new(AggregateNode::Variance(expr.into(), true)))
}

pub fn stdev<'a, T: Into<ExprNode<'a>>>(expr: T) -> ExprNode<'a> {
    ExprNode::Aggregate(Box::new(AggregateNode::Stdev(expr.into(), false)))
}

pub fn stdev_distinct<'a, T: Into<ExprNode<'a>>>(expr: T) -> ExprNode<'a> {
    ExprNode::Aggregate(Box::new(AggregateNode::Stdev(expr.into(), true)))
}

#[cfg(test)]
mod tests {
    use crate::ast_builder::{
        avg, avg_distinct, col, count, count_distinct, max, max_distinct, min, min_distinct, stdev,
        stdev_distinct, sum, sum_distinct, total, test_expr, variance, variance_distinct,
    };

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

        let actual = count_distinct("*");
        let expected = "COUNT(DISTINCT *)";
        test_expr(actual, expected);

        let actual = col("id").count_distinct();
        let expected = "COUNT(DISTINCT id)";
        test_expr(actual, expected);

        let actual = count_distinct("id");
        let expected = "COUNT(DISTINCT id)";
        test_expr(actual, expected);

        let actual = col("amount").sum();
        let expected = "SUM(amount)";
        test_expr(actual, expected);

        let actual = sum("amount");
        let expected = "SUM(amount)";
        test_expr(actual, expected);

        let actual = col("amount").sum_distinct();
        let expected = "SUM(DISTINCT amount)";
        test_expr(actual, expected);

        let actual = sum_distinct("amount");
        let expected = "SUM(DISTINCT amount)";
        test_expr(actual, expected);
      
        let actual = total("amount");
        let expected = "TOTAL(amount)";
        test_expr(actual, expected);

        let actual = col("budget").min();
        let expected = "MIN(budget)";
        test_expr(actual, expected);

        let actual = min("budget");
        let expected = "MIN(budget)";
        test_expr(actual, expected);

        let actual = col("budget").min_distinct();
        let expected = "MIN(DISTINCT budget)";
        test_expr(actual, expected);

        let actual = min_distinct("budget");
        let expected = "MIN(DISTINCT budget)";
        test_expr(actual, expected);

        let actual = col("score").max();
        let expected = "MAX(score)";
        test_expr(actual, expected);

        let actual = max("score");
        let expected = "MAX(score)";
        test_expr(actual, expected);

        let actual = col("grade").max_distinct();
        let expected = "MAX(DISTINCT grade)";
        test_expr(actual, expected);

        let actual = max_distinct("grade");
        let expected = "MAX(DISTINCT grade)";
        test_expr(actual, expected);

        let actual = col("grade").avg();
        let expected = "AVG(grade)";
        test_expr(actual, expected);

        let actual = avg("grade");
        let expected = "AVG(grade)";
        test_expr(actual, expected);

        let actual = col("grade").avg_distinct();
        let expected = "AVG(DISTINCT grade)";
        test_expr(actual, expected);

        let actual = avg_distinct("grade");
        let expected = "AVG(DISTINCT grade)";
        test_expr(actual, expected);

        let actual = col("statistic").variance();
        let expected = "VARIANCE(statistic)";
        test_expr(actual, expected);

        let actual = variance("statistic");
        let expected = "VARIANCE(statistic)";
        test_expr(actual, expected);

        let actual = col("statistic").variance_distinct();
        let expected = "VARIANCE(DISTINCT statistic)";
        test_expr(actual, expected);

        let actual = variance_distinct("statistic");
        let expected = "VARIANCE(DISTINCT statistic)";
        test_expr(actual, expected);

        let actual = col("scatterplot").stdev();
        let expected = "STDEV(scatterplot)";
        test_expr(actual, expected);

        let actual = stdev("scatterplot");
        let expected = "STDEV(scatterplot)";
        test_expr(actual, expected);

        let actual = col("scatterplot").stdev_distinct();
        let expected = "STDEV(DISTINCT scatterplot)";
        test_expr(actual, expected);

        let actual = stdev_distinct("scatterplot");
        let expected = "STDEV(DISTINCT scatterplot)";
        test_expr(actual, expected);
    }
}
