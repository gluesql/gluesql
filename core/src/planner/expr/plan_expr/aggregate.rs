use crate::plan::{AggregateExprPlan, AggregateFunctionPlan, CountArgExprPlan, ExprPlan};

impl AggregateExprPlan {
    pub fn as_expr(&self) -> Option<&ExprPlan> {
        match &self.func {
            AggregateFunctionPlan::Count(CountArgExprPlan::Wildcard) => None,
            AggregateFunctionPlan::Count(CountArgExprPlan::Expr(expr))
            | AggregateFunctionPlan::Sum(expr)
            | AggregateFunctionPlan::Max(expr)
            | AggregateFunctionPlan::Min(expr)
            | AggregateFunctionPlan::Avg(expr)
            | AggregateFunctionPlan::Variance(expr)
            | AggregateFunctionPlan::Stdev(expr) => Some(expr),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        parse_sql::parse_expr,
        plan::ExprPlan,
        translate::{NO_PARAMS, translate_expr},
    };

    #[test]
    fn as_expr() {
        macro_rules! test {
            ($input: literal, $expected: expr) => {
                let parsed = parse_expr($input).expect($input);
                let expr = ExprPlan::from(translate_expr(&parsed, NO_PARAMS).expect($input));
                let actual = match expr {
                    ExprPlan::Aggregate(aggregate) => Some(aggregate.as_expr().cloned()),
                    _ => None,
                };
                let expected: Option<&str> = $expected;
                let expected = expected.map(|expected| {
                    let parsed = parse_expr(expected).expect(expected);

                    ExprPlan::from(translate_expr(&parsed, NO_PARAMS).expect(expected))
                });
                let expected = Some(expected);

                assert_eq!(actual, expected, "input: {}", $input);
            };
        }

        test!("COUNT(*)", None);
        test!("COUNT(id)", Some("id"));
        test!("SUM(id)", Some("id"));
        test!("MAX(id)", Some("id"));
        test!("MIN(id)", Some("id"));
        test!("AVG(id)", Some("id"));
        test!("VARIANCE(id)", Some("id"));
        test!("STDEV(id)", Some("id"));
    }
}
