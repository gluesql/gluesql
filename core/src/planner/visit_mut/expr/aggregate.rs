use {
    super::super::VisitorMut,
    crate::plan::{AggregateExprPlan, AggregateFunctionPlan, CountArgExprPlan},
};

pub fn walk_aggregate_mut<V: VisitorMut + ?Sized>(
    visitor: &mut V,
    aggregate: &mut AggregateExprPlan,
) -> Result<(), V::Error> {
    match &mut aggregate.func {
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
        super::super::{try_visit_mut_expr, visit_mut_expr},
        crate::{
            parse_sql::parse_expr,
            plan::ExprPlan,
            translate::{NO_PARAMS, translate_expr},
        },
    };

    fn test(input: &str, expected: &str) {
        let parsed = parse_expr(input).expect(input);
        let mut expr = ExprPlan::from(translate_expr(&parsed, NO_PARAMS).expect(input));
        visit_mut_expr(&mut expr, &mut |e| {
            if let ExprPlan::Identifier(ident) = e {
                *e = ExprPlan::Identifier(format!("__{ident}__"));
            }
        });

        let expected_parsed = parse_expr(expected).expect(expected);
        let expected = ExprPlan::from(translate_expr(&expected_parsed, NO_PARAMS).expect(expected));

        assert_eq!(expr, expected, "\ninput: {input}\nexpected: {expected:?}");
    }

    #[test]
    fn visit_mut_aggregate_variants() {
        test("SUM(x)", "SUM(__x__)");
        test("COUNT(x)", "COUNT(__x__)");
        test("COUNT(*)", "COUNT(*)");
        test("MIN(x)", "MIN(__x__)");
        test("MAX(x)", "MAX(__x__)");
        test("AVG(x)", "AVG(__x__)");
        test("VARIANCE(x)", "VARIANCE(__x__)");
        test("STDEV(x)", "STDEV(__x__)");
    }

    #[test]
    fn try_visit_mut_aggregate_children_propagate_error() {
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
            let mut expr = ExprPlan::from(translate_expr(&parsed, NO_PARAMS).expect(sql));
            let mut visited = Vec::new();
            let actual = try_visit_mut_expr(&mut expr, &mut |expr| {
                visited.push(expr.clone());
                Err("stop")
            });
            assert_eq!(actual, Err("stop"), "{sql}");
            assert_eq!(visited, vec![ExprPlan::Identifier("x".to_owned())], "{sql}");
        }
    }
}
