use {
    super::{ExprPlan, fmt_expr},
    crate::{
        ast,
        plan::explain::{Explain, ExplainContext},
    },
    serde::{Deserialize, Serialize},
};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct AggregateExprPlan {
    pub func: AggregateFunctionPlan,
    pub distinct: bool,
    pub slot: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum AggregateFunctionPlan {
    Count(CountArgExprPlan),
    Sum(ExprPlan),
    Max(ExprPlan),
    Min(ExprPlan),
    Avg(ExprPlan),
    Variance(ExprPlan),
    Stdev(ExprPlan),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum CountArgExprPlan {
    Wildcard,
    Expr(ExprPlan),
}

impl From<ast::Aggregate> for AggregateExprPlan {
    fn from(aggregate: ast::Aggregate) -> Self {
        let ast::Aggregate { func, distinct } = aggregate;

        Self {
            func: func.into(),
            distinct,
            slot: None,
        }
    }
}

impl From<ast::AggregateFunction> for AggregateFunctionPlan {
    fn from(func: ast::AggregateFunction) -> Self {
        match func {
            ast::AggregateFunction::Count(expr) => Self::Count(expr.into()),
            ast::AggregateFunction::Sum(expr) => Self::Sum(expr.into()),
            ast::AggregateFunction::Max(expr) => Self::Max(expr.into()),
            ast::AggregateFunction::Min(expr) => Self::Min(expr.into()),
            ast::AggregateFunction::Avg(expr) => Self::Avg(expr.into()),
            ast::AggregateFunction::Variance(expr) => Self::Variance(expr.into()),
            ast::AggregateFunction::Stdev(expr) => Self::Stdev(expr.into()),
        }
    }
}

impl From<ast::CountArgExpr> for CountArgExprPlan {
    fn from(expr: ast::CountArgExpr) -> Self {
        match expr {
            ast::CountArgExpr::Wildcard => Self::Wildcard,
            ast::CountArgExpr::Expr(expr) => Self::Expr(expr.into()),
        }
    }
}

impl Explain for AggregateExprPlan {
    type Output = String;

    fn explain(&self, context: &mut ExplainContext) -> String {
        let mut output = String::new();
        let (name, expr) = match &self.func {
            AggregateFunctionPlan::Count(CountArgExprPlan::Wildcard) => ("COUNT", None),
            AggregateFunctionPlan::Count(CountArgExprPlan::Expr(expr)) => ("COUNT", Some(expr)),
            AggregateFunctionPlan::Sum(expr) => ("SUM", Some(expr)),
            AggregateFunctionPlan::Max(expr) => ("MAX", Some(expr)),
            AggregateFunctionPlan::Min(expr) => ("MIN", Some(expr)),
            AggregateFunctionPlan::Avg(expr) => ("AVG", Some(expr)),
            AggregateFunctionPlan::Variance(expr) => ("VARIANCE", Some(expr)),
            AggregateFunctionPlan::Stdev(expr) => ("STDEV", Some(expr)),
        };
        output.push_str(name);
        output.push('(');
        if self.distinct {
            output.push_str("DISTINCT ");
        }
        match expr {
            Some(expr) => fmt_expr(expr, context, &mut output),
            None => output.push('*'),
        }
        output.push(')');
        output
    }
}

impl Explain for [AggregateExprPlan] {
    type Output = String;

    fn explain(&self, context: &mut ExplainContext) -> String {
        let mut output = String::new();
        for (index, aggregate) in self.iter().enumerate() {
            if index > 0 {
                output.push_str(", ");
            }
            output.push_str(&aggregate.explain(context));
        }
        output
    }
}

#[cfg(test)]
mod tests {
    use {
        super::{AggregateExprPlan, AggregateFunctionPlan, CountArgExprPlan},
        crate::plan::{
            ExprPlan,
            explain::{Explain, ExplainContext},
        },
    };

    #[test]
    fn explain() {
        let actual = [
            AggregateExprPlan {
                func: AggregateFunctionPlan::Count(CountArgExprPlan::Wildcard),
                distinct: false,
                slot: Some(0),
            },
            AggregateExprPlan {
                func: AggregateFunctionPlan::Count(CountArgExprPlan::Expr(ExprPlan::Identifier(
                    "id".to_owned(),
                ))),
                distinct: true,
                slot: Some(1),
            },
            AggregateExprPlan {
                func: AggregateFunctionPlan::Sum(ExprPlan::Identifier("score".to_owned())),
                distinct: false,
                slot: Some(2),
            },
            AggregateExprPlan {
                func: AggregateFunctionPlan::Max(ExprPlan::Identifier("score".to_owned())),
                distinct: false,
                slot: Some(3),
            },
            AggregateExprPlan {
                func: AggregateFunctionPlan::Min(ExprPlan::Identifier("score".to_owned())),
                distinct: false,
                slot: Some(4),
            },
            AggregateExprPlan {
                func: AggregateFunctionPlan::Avg(ExprPlan::Identifier("score".to_owned())),
                distinct: false,
                slot: Some(5),
            },
            AggregateExprPlan {
                func: AggregateFunctionPlan::Variance(ExprPlan::Identifier("score".to_owned())),
                distinct: false,
                slot: Some(6),
            },
            AggregateExprPlan {
                func: AggregateFunctionPlan::Stdev(ExprPlan::Identifier("score".to_owned())),
                distinct: false,
                slot: Some(7),
            },
        ];
        let expected = "COUNT(*), COUNT(DISTINCT id), SUM(score), MAX(score), MIN(score), AVG(score), VARIANCE(score), STDEV(score)";

        assert_eq!(
            actual.as_slice().explain(&mut ExplainContext::default()),
            expected
        );
    }
}
