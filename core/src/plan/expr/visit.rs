mod aggregate;
mod function;

use crate::ast::Expr;

use aggregate::{try_visit_aggregate, visit_mut_aggregate};
use function::{try_visit_function, visit_mut_function};

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

macro_rules! visit_expr_children {
    ($expr:expr, $visit_expr:ident, $visit_function:ident, $visit_aggregate:ident, $f:expr, $apply:ident) => {
        match $expr {
            Expr::Identifier(_)
            | Expr::CompoundIdentifier { .. }
            | Expr::Literal(_)
            | Expr::Value(_)
            | Expr::TypedString { .. }
            | Expr::Exists { .. }
            | Expr::Subquery(_) => {}
            Expr::IsNull(inner) | Expr::IsNotNull(inner) | Expr::Nested(inner) => {
                $apply!($visit_expr(inner, $f));
            }
            Expr::InList { expr, list, .. } => {
                $apply!($visit_expr(expr, $f));
                for e in list {
                    $apply!($visit_expr(e, $f));
                }
            }
            Expr::InSubquery { expr, .. }
            | Expr::UnaryOp { expr, .. }
            | Expr::Interval { expr, .. } => {
                $apply!($visit_expr(expr, $f));
            }
            Expr::Between {
                expr, low, high, ..
            } => {
                $apply!($visit_expr(expr, $f));
                $apply!($visit_expr(low, $f));
                $apply!($visit_expr(high, $f));
            }
            Expr::Like { expr, pattern, .. } | Expr::ILike { expr, pattern, .. } => {
                $apply!($visit_expr(expr, $f));
                $apply!($visit_expr(pattern, $f));
            }
            Expr::BinaryOp { left, right, .. } => {
                $apply!($visit_expr(left, $f));
                $apply!($visit_expr(right, $f));
            }
            Expr::Function(func) => {
                $apply!($visit_function(func, $f));
            }
            Expr::Aggregate(aggr) => {
                $apply!($visit_aggregate(aggr, $f));
            }
            Expr::Case {
                operand,
                when_then,
                else_result,
            } => {
                if let Some(e) = operand {
                    $apply!($visit_expr(e, $f));
                }
                for (when, then) in when_then {
                    $apply!($visit_expr(when, $f));
                    $apply!($visit_expr(then, $f));
                }
                if let Some(e) = else_result {
                    $apply!($visit_expr(e, $f));
                }
            }
            Expr::ArrayIndex { obj, indexes } => {
                $apply!($visit_expr(obj, $f));
                for e in indexes {
                    $apply!($visit_expr(e, $f));
                }
            }
            Expr::Array { elem } => {
                for e in elem {
                    $apply!($visit_expr(e, $f));
                }
            }
        }
    };
}

pub fn visit_mut_expr<F>(expr: &mut Expr, f: &mut F)
where
    F: FnMut(&mut Expr),
{
    visit_expr_children!(
        expr,
        visit_mut_expr,
        visit_mut_function,
        visit_mut_aggregate,
        f,
        apply_mut
    );

    f(expr);
}

pub fn try_visit_expr<E, F>(expr: &Expr, f: &mut F) -> Result<(), E>
where
    F: FnMut(&Expr) -> Result<(), E>,
{
    visit_expr_children!(
        expr,
        try_visit_expr,
        try_visit_function,
        try_visit_aggregate,
        f,
        apply_try
    );
    f(expr)
}

#[cfg(test)]
mod tests {
    use {
        super::{try_visit_expr, visit_mut_expr},
        crate::{
            ast::Expr,
            parse_sql::parse_expr,
            plan::PlanError,
            translate::{NO_PARAMS, translate_expr},
        },
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
    fn visit_mut_expr_variants() {
        test("id", "_id");
        test("t.id", "t.id");
        test("id IS NULL", "_id IS NULL");
        test("id IS NOT NULL", "_id IS NOT NULL");
        test("id IN (a, b, c)", "_id IN (_a, _b, _c)");
        test("id IN (SELECT 1)", "_id IN (SELECT 1)");
        test("id BETWEEN low AND high", "_id BETWEEN _low AND _high");
        test("name LIKE pattern", "_name LIKE _pattern");
        test("name ILIKE pattern", "_name ILIKE _pattern");
        test("a + b", "_a + _b");
        test("-x", "-_x");
        test("(id)", "(_id)");
        test("123", "123");
        test("TRUE", "TRUE");
        test("NULL", "NULL");
        test("INT '123'", "INT '123'");
        test("ABS(x)", "ABS(_x)");
        test("SUM(x)", "SUM(_x)");
        test("EXISTS(SELECT 1)", "EXISTS(SELECT 1)");
        test("(SELECT x)", "(SELECT x)");
        test(
            "CASE WHEN a THEN b ELSE c END",
            "CASE WHEN _a THEN _b ELSE _c END",
        );
        test(
            "CASE x WHEN 1 THEN a ELSE b END",
            "CASE _x WHEN 1 THEN _a ELSE _b END",
        );
        test("arr[idx]", "_arr[_idx]");
        test("INTERVAL x DAY", "INTERVAL _x DAY");
        test("[a, b, c]", "[_a, _b, _c]");
    }

    #[test]
    fn try_visit_expr_propagates_error() {
        let parsed = parse_expr("a + b").expect("a + b");
        let expr = translate_expr(&parsed, NO_PARAMS).expect("a + b");

        let result = try_visit_expr(&expr, &mut |expr| match expr {
            Expr::Identifier(ident) if ident == "b" => Err(PlanError::Unreachable),
            _ => Ok(()),
        });

        assert_eq!(result, Err(PlanError::Unreachable));
    }

    #[test]
    fn try_visit_expr_short_circuits_after_error() {
        let parsed = parse_expr("(a + b) + c").expect("(a + b) + c");
        let expr = translate_expr(&parsed, NO_PARAMS).expect("(a + b) + c");
        let mut visited = Vec::new();

        let result = try_visit_expr(&expr, &mut |expr| match expr {
            Expr::Identifier(ident) => {
                visited.push(ident.clone());
                if ident == "b" {
                    Err(PlanError::Unreachable)
                } else {
                    Ok(())
                }
            }
            _ => Ok(()),
        });

        assert_eq!(result, Err(PlanError::Unreachable));
        assert_eq!(visited, vec!["a".to_owned(), "b".to_owned()]);
    }
}
