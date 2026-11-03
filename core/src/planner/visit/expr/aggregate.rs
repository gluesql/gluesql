use {
    super::super::Visitor,
    crate::plan::{AggregateExprPlan, AggregateFunctionPlan, CountArgExprPlan},
};

pub fn walk_aggregate<V: Visitor + ?Sized>(
    visitor: &mut V,
    aggregate: &AggregateExprPlan,
) -> Result<(), V::Error> {
    match &aggregate.func {
        AggregateFunctionPlan::Count(count_arg) => {
            if let CountArgExprPlan::Expr(expr) = count_arg {
                visitor.visit_expr(expr)?;
            }
        }
        AggregateFunctionPlan::Sum(expr)
        | AggregateFunctionPlan::Min(expr)
        | AggregateFunctionPlan::Max(expr)
        | AggregateFunctionPlan::Avg(expr)
        | AggregateFunctionPlan::Variance(expr)
        | AggregateFunctionPlan::Stdev(expr) => {
            visitor.visit_expr(expr)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use {
        super::super::try_visit_expr,
        crate::{
            parse_sql::parse_expr,
            plan::ExprPlan,
            translate::{NO_PARAMS, translate_expr},
        },
    };

    fn test(input: &str, expected: &[&str]) {
        let parsed = parse_expr(input).expect(input);
        let expr = ExprPlan::from(translate_expr(&parsed, NO_PARAMS).expect(input));
        let mut visited = Vec::new();
        let result: Result<(), &str> = try_visit_expr(&expr, &mut |expr| {
            if let ExprPlan::Identifier(name) = expr {
                visited.push(name.clone());
            }
            Ok(())
        });
        assert_eq!(result, Ok(()), "{input}");
        assert_eq!(visited, expected, "{input}");
    }

    #[test]
    fn visit_aggregate_variants() {
        test("SUM(x)", &["x"]);
        test("COUNT(x)", &["x"]);
        test("COUNT(*)", &[]);
        test("MIN(x)", &["x"]);
        test("MAX(x)", &["x"]);
        test("AVG(x)", &["x"]);
        test("VARIANCE(x)", &["x"]);
        test("STDEV(x)", &["x"]);
    }

    #[test]
    fn try_visit_aggregate_children_propagate_error() {
        for sql in [
            "COUNT(x)",
            "SUM(x)",
            "MIN(x)",
            "MAX(x)",
            "AVG(x)",
            "VARIANCE(x)",
            "STDEV(x)",
        ] {
            let parsed = parse_expr(sql).expect(sql);
            let expr = ExprPlan::from(translate_expr(&parsed, NO_PARAMS).expect(sql));
            let mut visited = Vec::new();
            let actual = try_visit_expr(&expr, &mut |expr| {
                visited.push(expr.clone());
                Err("stop")
            });
            assert_eq!(actual, Err("stop"), "{sql}");
            assert_eq!(visited, vec![ExprPlan::Identifier("x".to_owned())], "{sql}");
        }
    }
}
