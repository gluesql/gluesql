mod aggregate;
mod function;

use crate::ast::Expr;

use aggregate::visit_mut_aggregate;
use function::visit_mut_function;

pub fn visit_mut_expr<F>(expr: &mut Expr, f: &mut F)
where
    F: FnMut(&mut Expr),
{
    match expr {
        Expr::Identifier(_)
        | Expr::CompoundIdentifier { .. }
        | Expr::Literal(_)
        | Expr::Value(_)
        | Expr::TypedString { .. }
        | Expr::Exists { .. }
        | Expr::Subquery(_) => {}
        Expr::IsNull(inner) | Expr::IsNotNull(inner) | Expr::Nested(inner) => {
            visit_mut_expr(inner, f);
        }
        Expr::InList { expr, list, .. } => {
            visit_mut_expr(expr, f);
            for e in list {
                visit_mut_expr(e, f);
            }
        }
        Expr::InSubquery { expr, .. }
        | Expr::UnaryOp { expr, .. }
        | Expr::Interval { expr, .. } => {
            visit_mut_expr(expr, f);
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            visit_mut_expr(expr, f);
            visit_mut_expr(low, f);
            visit_mut_expr(high, f);
        }
        Expr::Like { expr, pattern, .. } | Expr::ILike { expr, pattern, .. } => {
            visit_mut_expr(expr, f);
            visit_mut_expr(pattern, f);
        }
        Expr::BinaryOp { left, right, .. } => {
            visit_mut_expr(left, f);
            visit_mut_expr(right, f);
        }
        Expr::Function(func) => visit_mut_function(func, f),
        Expr::Aggregate(aggr) => visit_mut_aggregate(aggr, f),
        Expr::Case {
            operand,
            when_then,
            else_result,
        } => {
            if let Some(e) = operand {
                visit_mut_expr(e, f);
            }
            for (when, then) in when_then {
                visit_mut_expr(when, f);
                visit_mut_expr(then, f);
            }
            if let Some(e) = else_result {
                visit_mut_expr(e, f);
            }
        }
        Expr::ArrayIndex { obj, indexes } => {
            visit_mut_expr(obj, f);
            for e in indexes {
                visit_mut_expr(e, f);
            }
        }
        Expr::Array { elem } => {
            for e in elem {
                visit_mut_expr(e, f);
            }
        }
    }

    f(expr);
}

#[cfg(test)]
mod tests {
    use {
        super::visit_mut_expr,
        crate::{
            ast::Expr,
            parse_sql::parse_expr,
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
}
