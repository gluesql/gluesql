use crate::{
    ast::{Aggregate, AggregateFunction, CountArgExpr, Expr},
    plan::PlanError,
};

use super::{try_visit_expr, visit_mut_expr};

macro_rules! apply_mut {
    ($visit:expr) => {
        $visit
    };
}

macro_rules! apply_try {
    ($visit:expr) => {
        $visit?
    };
}

macro_rules! visit_aggregate_children {
    ($func:expr, $visit_expr:ident, $f:expr, $apply:ident) => {
        match $func {
            AggregateFunction::Count(count_arg) => {
                if let CountArgExpr::Expr(expr) = count_arg {
                    $apply!($visit_expr(expr, $f));
                }
            }
            AggregateFunction::Sum(expr)
            | AggregateFunction::Min(expr)
            | AggregateFunction::Max(expr)
            | AggregateFunction::Avg(expr)
            | AggregateFunction::Variance(expr)
            | AggregateFunction::Stdev(expr) => {
                $apply!($visit_expr(expr, $f));
            }
        }
    };
}

pub fn visit_mut_aggregate<F>(aggr: &mut Aggregate, f: &mut F)
where
    F: FnMut(&mut Expr),
{
    visit_aggregate_children!(&mut aggr.func, visit_mut_expr, f, apply_mut);
}

pub fn try_visit_aggregate<F>(aggr: &Aggregate, f: &mut F) -> Result<(), PlanError>
where
    F: FnMut(&Expr) -> Result<(), PlanError>,
{
    visit_aggregate_children!(&aggr.func, try_visit_expr, f, apply_try);
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::{
        ast::Expr,
        parse_sql::parse_expr,
        plan::{
            PlanError,
            expr::{try_visit_expr, visit_mut_expr},
        },
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

    #[test]
    fn try_visit_aggregate_propagates_error() {
        let parsed = parse_expr("SUM(x)").expect("SUM(x)");
        let expr = translate_expr(&parsed, NO_PARAMS).expect("SUM(x)");

        let result = try_visit_expr(&expr, &mut |expr| match expr {
            Expr::Identifier(ident) if ident == "x" => Err(PlanError::Unreachable),
            _ => Ok(()),
        });

        assert_eq!(result, Err(PlanError::Unreachable));
    }
}
