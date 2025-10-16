use crate::ast::{Expr, Function};

pub fn is_deterministic(expr: &Expr) -> bool {
    match expr {
        Expr::Literal(_) | Expr::TypedString { .. } => true,
        Expr::Identifier(_) | Expr::CompoundIdentifier { .. } => false,
        Expr::IsNull(inner)
        | Expr::IsNotNull(inner)
        | Expr::UnaryOp { expr: inner, .. }
        | Expr::Nested(inner)
        | Expr::Interval { expr: inner, .. } => is_deterministic(inner),
        Expr::BinaryOp { left, right, .. }
        | Expr::Like {
            expr: left,
            pattern: right,
            ..
        }
        | Expr::ILike {
            expr: left,
            pattern: right,
            ..
        } => is_deterministic(left) && is_deterministic(right),
        Expr::Between {
            expr, low, high, ..
        } => is_deterministic(expr) && is_deterministic(low) && is_deterministic(high),
        Expr::InList { expr, list, .. } => {
            is_deterministic(expr) && list.iter().all(is_deterministic)
        }
        Expr::Function(function) => is_function_deterministic(function),
        Expr::Case {
            operand,
            when_then,
            else_result,
        } => {
            operand.as_deref().map(is_deterministic).unwrap_or(true)
                && when_then
                    .iter()
                    .all(|(when, then)| is_deterministic(when) && is_deterministic(then))
                && else_result.as_deref().map(is_deterministic).unwrap_or(true)
        }
        Expr::Array { elem } => elem.iter().all(is_deterministic),
        Expr::ArrayIndex { obj, indexes } => {
            is_deterministic(obj) && indexes.iter().all(is_deterministic)
        }
        Expr::Subquery(_) | Expr::Exists { .. } | Expr::InSubquery { .. } | Expr::Aggregate(_) => {
            false
        }
    }
}

fn is_function_deterministic(function: &Function) -> bool {
    match function {
        Function::Cast { expr, .. } => is_deterministic(expr),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use {
        super::is_deterministic,
        crate::{parse_sql::parse_expr, translate::translate_expr},
    };

    fn expr(sql: &str) -> &str {
        sql
    }

    fn assert_deterministic(sql: &str, expected: bool) {
        let expr = parse_expr(sql).and_then(|parsed| translate_expr(&parsed));
        let actual = expr.map(|expr| is_deterministic(&expr));

        assert_eq!(actual, Ok(expected), "{sql} deterministic mismatch");
    }

    #[test]
    fn deterministic_cases() {
        let deterministic_cases = [
            expr("1"),
            expr("INTERVAL 1 DAY"),
            expr("-(1 + 2)"),
            expr("('A' IS NULL)"),
            expr("CAST(1 AS INT)"),
            expr("(1 BETWEEN 0 AND 2)"),
            expr("1 + (2 * 3)"),
            expr("('A' LIKE 'A%')"),
            expr("('A' ILIKE 'A%')"),
            expr("ARRAY[1, 2, 3]"),
            expr("ARRAY['A', 'B'][1]"),
            expr("CASE 1 WHEN 1 THEN 2 ELSE 3 END"),
            expr("('A' IN ('A', 'B'))"),
        ];

        for sql in deterministic_cases {
            assert_deterministic(sql, true);
        }
    }

    #[test]
    fn non_deterministic_cases() {
        let non_deterministic = [
            expr("id"),
            expr("Foo.id"),
            expr("NOW()"),
            expr("RAND()"),
            expr("(SELECT 1)"),
            expr("EXISTS (SELECT 1)"),
            expr("1 IN (SELECT 1)"),
            expr("SUM(id)"),
        ];

        for sql in non_deterministic {
            assert_deterministic(sql, false);
        }
    }

    #[test]
    #[should_panic(expected = "deterministic mismatch")]
    fn invalid_expression_panics() {
        assert_deterministic("(+", false);
    }
}
