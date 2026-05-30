use crate::plan::{AggregateFunctionPlan, AggregatePlan, CountArgExprPlan, ExprPlan};

impl AggregatePlan {
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
        plan::{AggregatePlan, ExprPlan},
        translate::{NO_PARAMS, translate_expr},
    };

    fn parse(sql: &str) -> AggregatePlan {
        let parsed = parse_expr(sql).unwrap();
        let expr = ExprPlan::from(translate_expr(&parsed, NO_PARAMS).unwrap());

        match expr {
            ExprPlan::Aggregate(aggregate) => *aggregate,
            _ => unreachable!("only for aggregate tests"),
        }
    }

    #[test]
    fn as_expr() {
        assert_eq!(parse("COUNT(*)").as_expr(), None);

        let actual = parse("COUNT(id)");
        let expected = ExprPlan::Identifier("id".to_owned());
        assert_eq!(actual.as_expr(), Some(&expected));

        let actual = parse("SUM(id)");
        let expected = ExprPlan::Identifier("id".to_owned());
        assert_eq!(actual.as_expr(), Some(&expected));

        let actual = parse("MAX(id)");
        let expected = ExprPlan::Identifier("id".to_owned());
        assert_eq!(actual.as_expr(), Some(&expected));

        let actual = parse("MIN(id)");
        let expected = ExprPlan::Identifier("id".to_owned());
        assert_eq!(actual.as_expr(), Some(&expected));

        let actual = parse("AVG(id)");
        let expected = ExprPlan::Identifier("id".to_owned());
        assert_eq!(actual.as_expr(), Some(&expected));

        let actual = parse("VARIANCE(id)");
        let expected = ExprPlan::Identifier("id".to_owned());
        assert_eq!(actual.as_expr(), Some(&expected));
    }
}
