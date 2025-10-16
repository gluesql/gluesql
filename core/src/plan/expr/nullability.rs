use crate::ast::{AstLiteral, Expr, Function};

pub fn may_return_null(expr: &Expr) -> bool {
    match expr {
        Expr::Literal(AstLiteral::Null) => true,
        Expr::Literal(_) | Expr::TypedString { .. } => false,
        Expr::Identifier(_) | Expr::CompoundIdentifier { .. } => true,
        Expr::IsNull(_) | Expr::IsNotNull(_) => false,
        Expr::UnaryOp { expr: inner, .. }
        | Expr::Nested(inner)
        | Expr::Interval { expr: inner, .. } => may_return_null(inner),
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
        } => may_return_null(left) || may_return_null(right),
        Expr::Between {
            expr, low, high, ..
        } => may_return_null(expr) || may_return_null(low) || may_return_null(high),
        Expr::InList { expr, list, .. } => {
            may_return_null(expr) || list.iter().any(may_return_null)
        }
        Expr::Function(function) => function_may_return_null(function),
        Expr::Case {
            operand,
            when_then,
            else_result,
        } => {
            operand.as_deref().map(may_return_null).unwrap_or(false)
                || when_then
                    .iter()
                    .any(|(when, then)| may_return_null(when) || may_return_null(then))
                || else_result.as_deref().map(may_return_null).unwrap_or(true)
        }
        Expr::Array { elem } => elem.iter().any(may_return_null),
        Expr::ArrayIndex { .. }
        | Expr::Subquery(_)
        | Expr::Exists { .. }
        | Expr::InSubquery { .. }
        | Expr::Aggregate(_) => true,
    }
}

fn function_may_return_null(function: &Function) -> bool {
    match function {
        Function::Cast { expr, .. } => may_return_null(expr),
        _ => true,
    }
}

#[cfg(test)]
mod tests {
    use {
        super::may_return_null,
        crate::{parse_sql::parse_expr, translate::translate_expr},
    };

    fn assert_nullability(sql: &str, expected: bool) {
        let parsed = parse_expr(sql).expect(sql);
        let expr = translate_expr(&parsed).expect(sql);

        assert_eq!(
            may_return_null(&expr),
            expected,
            "{sql} nullability mismatch"
        );
    }

    #[test]
    fn nullable_cases() {
        let cases = [
            "NULL",
            "id",
            "Foo.id",
            "CASE 1 WHEN 1 THEN id ELSE 0 END",
            "1 IN (SELECT 1)",
            "ARRAY[1, id]",
            "POSITION('a' IN id)",
        ];

        for sql in cases {
            assert_nullability(sql, true);
        }
    }

    #[test]
    fn non_nullable_cases() {
        let cases = [
            "1",
            "'A'",
            "INTERVAL 1 DAY",
            "NOT TRUE",
            "1 BETWEEN 0 AND 2",
            "'A' LIKE 'A%'",
            "('A' IS NULL)",
            "CAST(1 AS INT)",
            "CASE 1 WHEN 1 THEN 2 ELSE 3 END",
        ];

        for sql in cases {
            assert_nullability(sql, false);
        }
    }
}
