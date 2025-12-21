use crate::ast::{Aggregate, AggregateFunction, CountArgExpr, Expr};

use super::visit_mut_expr;

pub fn visit_mut_aggregate<F>(aggr: &mut Aggregate, f: &mut F)
where
    F: FnMut(&mut Expr),
{
    match &mut aggr.func {
        AggregateFunction::Count(count_arg) => {
            if let CountArgExpr::Expr(expr) = count_arg {
                visit_mut_expr(expr, f);
            }
        }
        AggregateFunction::Sum(expr)
        | AggregateFunction::Min(expr)
        | AggregateFunction::Max(expr)
        | AggregateFunction::Avg(expr)
        | AggregateFunction::Variance(expr)
        | AggregateFunction::Stdev(expr) => {
            visit_mut_expr(expr, f);
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        ast::Expr,
        parse_sql::parse_expr,
        plan::expr::visit_mut_expr,
        translate::{NO_PARAMS, translate_expr},
    };

    fn test(input: &str, expected: &str) {
        let parsed = parse_expr(input).expect(input);
        let mut expr = translate_expr(&parsed, NO_PARAMS).expect(input);

        visit_mut_expr(&mut expr, &mut |e| {
            if let Expr::Identifier(ident) = e {
                *e = Expr::Identifier(format!("_{ident}"));
            }
        });

        let expected_parsed = parse_expr(expected).expect(expected);
        let expected = translate_expr(&expected_parsed, NO_PARAMS).expect(expected);

        assert_eq!(expr, expected, "\ninput: {input}\nexpected: {expected:?}");
    }

    #[test]
    fn visit_mut_aggregate_variants() {
        test("SUM(x)", "SUM(_x)");
        test("COUNT(x)", "COUNT(_x)");
        test("COUNT(*)", "COUNT(*)");
        test("MIN(x)", "MIN(_x)");
        test("MAX(x)", "MAX(_x)");
        test("AVG(x)", "AVG(_x)");
    }
}
