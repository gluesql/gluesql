mod aggregate;
mod function;

pub(super) use aggregate::walk_aggregate;

use {
    super::Visitor,
    crate::plan::{ExprPlan, QueryPlan},
    function::walk_function,
};

pub fn walk_expr<V: Visitor + ?Sized>(visitor: &mut V, expr: &ExprPlan) -> Result<(), V::Error> {
    match expr {
        ExprPlan::Identifier(_)
        | ExprPlan::CompoundIdentifier { .. }
        | ExprPlan::Literal(_)
        | ExprPlan::Value(_)
        | ExprPlan::TypedString { .. } => {}
        ExprPlan::Subquery(query)
        | ExprPlan::Exists {
            subquery: query, ..
        } => {
            visitor.visit_query(query)?;
        }
        ExprPlan::InSubquery { expr, subquery, .. } => {
            visitor.visit_expr(expr)?;
            visitor.visit_query(subquery)?;
        }
        ExprPlan::IsNull(inner) | ExprPlan::IsNotNull(inner) | ExprPlan::Nested(inner) => {
            visitor.visit_expr(inner)?;
        }
        ExprPlan::InList { expr, list, .. } => {
            visitor.visit_expr(expr)?;
            for e in list {
                visitor.visit_expr(e)?;
            }
        }
        ExprPlan::UnaryOp { expr, .. } | ExprPlan::Interval { expr, .. } => {
            visitor.visit_expr(expr)?;
        }
        ExprPlan::Between {
            expr, low, high, ..
        } => {
            visitor.visit_expr(expr)?;
            visitor.visit_expr(low)?;
            visitor.visit_expr(high)?;
        }
        ExprPlan::Like { expr, pattern, .. }
        | ExprPlan::ILike { expr, pattern, .. }
        | ExprPlan::Regex { expr, pattern, .. } => {
            visitor.visit_expr(expr)?;
            visitor.visit_expr(pattern)?;
        }
        ExprPlan::BinaryOp { left, right, .. } => {
            visitor.visit_expr(left)?;
            visitor.visit_expr(right)?;
        }
        ExprPlan::Function(func) => {
            walk_function(visitor, func)?;
        }
        ExprPlan::Aggregate(aggr) => {
            walk_aggregate(visitor, aggr)?;
        }
        ExprPlan::Case {
            operand,
            when_then,
            else_result,
        } => {
            if let Some(e) = operand {
                visitor.visit_expr(e)?;
            }
            for (when, then) in when_then {
                visitor.visit_expr(when)?;
                visitor.visit_expr(then)?;
            }
            if let Some(e) = else_result {
                visitor.visit_expr(e)?;
            }
        }
        ExprPlan::ArrayIndex { obj, indexes } => {
            visitor.visit_expr(obj)?;
            for e in indexes {
                visitor.visit_expr(e)?;
            }
        }
        ExprPlan::Array { elem } => {
            for e in elem {
                visitor.visit_expr(e)?;
            }
        }
    }
    Ok(())
}

struct ExprVisitor<'a, F> {
    callback: &'a mut F,
}

impl<F, E> Visitor for ExprVisitor<'_, F>
where
    F: FnMut(&ExprPlan) -> Result<(), E>,
{
    type Error = E;

    fn visit_expr(&mut self, expr: &ExprPlan) -> Result<(), E> {
        walk_expr(self, expr)?;
        (self.callback)(expr)
    }

    fn visit_query(&mut self, _: &QueryPlan) -> Result<(), E> {
        Ok(())
    }
}

// Expression-only adapters preserve postorder callbacks and stop at query boundaries.
pub fn try_visit_expr<F, E>(expr: &ExprPlan, f: &mut F) -> Result<(), E>
where
    F: FnMut(&ExprPlan) -> Result<(), E>,
{
    Visitor::visit_expr(&mut ExprVisitor { callback: f }, expr)
}

#[cfg(test)]
mod tests {
    use {
        super::{Visitor, try_visit_expr, walk_expr},
        crate::{
            parse_sql::parse_expr,
            plan::{ExprPlan, QueryPlan},
            translate::{NO_PARAMS, translate_expr},
        },
    };

    fn expr(sql: &str) -> ExprPlan {
        translate_expr(&parse_expr(sql).expect(sql), NO_PARAMS)
            .expect(sql)
            .into()
    }

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
    fn visit_expr_variants() {
        test("id", &["id"]);
        test("t.id", &[]);
        test("id IS NULL", &["id"]);
        test("id IS NOT NULL", &["id"]);
        test("id IN (a, b, c)", &["id", "a", "b", "c"]);
        test("id IN (SELECT 1)", &["id"]);
        test("id BETWEEN low AND high", &["id", "low", "high"]);
        test("name LIKE pattern", &["name", "pattern"]);
        test("name ILIKE pattern", &["name", "pattern"]);
        test("name ~* pattern", &["name", "pattern"]);
        test("a + b", &["a", "b"]);
        test("-x", &["x"]);
        test("(id)", &["id"]);
        test("123", &[]);
        test("TRUE", &[]);
        test("NULL", &[]);
        test("INT '123'", &[]);
        test("ABS(x)", &["x"]);
        test("SUM(x)", &["x"]);
        test("EXISTS(SELECT 1)", &[]);
        test("(SELECT x)", &[]);
        test("CASE WHEN a THEN b ELSE c END", &["a", "b", "c"]);
        test("CASE x WHEN 1 THEN a ELSE b END", &["x", "a", "b"]);
        test("CASE WHEN a THEN b END", &["a", "b"]);
        test("arr[idx]", &["arr", "idx"]);
        test("INTERVAL x DAY", &["x"]);
        test("[a, b, c]", &["a", "b", "c"]);
    }

    #[test]
    fn try_visit_expr_short_circuits_after_error() {
        let parsed = parse_expr("(a + b) + c").expect("(a + b) + c");
        let expr = ExprPlan::from(translate_expr(&parsed, NO_PARAMS).expect("(a + b) + c"));
        let mut visited = Vec::new();

        let result = try_visit_expr(&expr, &mut |expr| match expr {
            ExprPlan::Identifier(ident) => {
                visited.push(ident.clone());
                if ident == "b" { Err("stop") } else { Ok(()) }
            }
            _ => Ok(()),
        });

        assert_eq!(result, Err("stop"));
        assert_eq!(visited, vec!["a".to_owned(), "b".to_owned()]);
    }

    #[test]
    fn try_visit_expr_children_short_circuit_after_error() {
        let cases: &[(&str, &[&str])] = &[
            ("a IS NULL", &["a"]),
            ("a IS NOT NULL", &["a"]),
            ("a + b", &["a", "b"]),
            ("a IN (b, c)", &["a", "b", "c"]),
            ("a IN (SELECT 1)", &["a"]),
            ("a BETWEEN b AND c", &["a", "b", "c"]),
            ("a LIKE b", &["a", "b"]),
            ("a ILIKE b", &["a", "b"]),
            ("a ~ b", &["a", "b"]),
            ("-a", &["a"]),
            ("(a)", &["a"]),
            ("a[b][c]", &["a", "b", "c"]),
            ("INTERVAL a DAY", &["a"]),
            ("[a, b]", &["a", "b"]),
            ("CASE a WHEN b THEN c ELSE d END", &["a", "b", "c", "d"]),
            ("CASE WHEN a THEN b END", &["a", "b"]),
            ("ABS(a)", &["a"]),
            ("SUM(a)", &["a"]),
        ];
        for &(sql, expected) in cases {
            for end in 0..expected.len() {
                let parsed = parse_expr(sql).expect(sql);
                let expr = ExprPlan::from(translate_expr(&parsed, NO_PARAMS).expect(sql));
                let mut visited = Vec::new();
                let actual = try_visit_expr(&expr, &mut |expr| {
                    if let ExprPlan::Identifier(name) = expr {
                        visited.push(name.clone());
                        if name == expected[end] {
                            return Err("stop");
                        }
                    }
                    Ok(())
                });
                assert_eq!(actual, Err("stop"), "{sql}: stop at {}", expected[end]);
                assert_eq!(visited, expected[..=end], "{sql}");
            }
        }
    }

    #[derive(Default)]
    struct Identifiers(Vec<String>);

    impl Visitor for Identifiers {
        type Error = &'static str;

        fn visit_expr(&mut self, expr: &ExprPlan) -> Result<(), Self::Error> {
            walk_expr(self, expr)?;
            if let ExprPlan::Identifier(name) = expr {
                self.0.push(name.clone());
            }
            Ok(())
        }
    }

    #[test]
    fn subqueries() {
        let cases: &[(&str, &[&str], &[&str])] = &[
            ("(SELECT x AS r)", &["x"], &[]),
            ("EXISTS(SELECT x AS r FROM SERIES(n))", &["n", "x"], &[]),
            (
                "x IN (SELECT y AS r FROM (VALUES (z)) v)",
                &["x", "z", "y"],
                &["x"],
            ),
            (
                "ABS((SELECT x AS r FROM SERIES((SELECT n AS s))))",
                &["n", "x"],
                &[],
            ),
            ("SUM((SELECT x AS r))", &["x"], &[]),
        ];
        for &(input, identifiers, outer_identifiers) in cases {
            let actual = expr(input);
            let mut read = Identifiers::default();
            assert_eq!(Visitor::visit_expr(&mut read, &actual), Ok(()), "{input}");
            assert_eq!(read.0, identifiers, "{input}");

            let mut visited = Vec::new();
            let result: Result<(), &str> = try_visit_expr(&expr(input), &mut |expr| {
                if let ExprPlan::Identifier(name) = expr {
                    visited.push(name.clone());
                }
                Ok(())
            });
            assert_eq!(result, Ok(()));
            assert_eq!(visited, outer_identifiers, "expression-only: {input}");
        }
    }

    #[test]
    fn subquery_errors() {
        struct RejectQueries;

        impl Visitor for RejectQueries {
            type Error = &'static str;

            fn visit_query(&mut self, _: &QueryPlan) -> Result<(), Self::Error> {
                Err("query")
            }
        }

        for sql in ["(SELECT x)", "EXISTS(SELECT x)", "x IN (SELECT y)"] {
            let input = expr(sql);
            assert_eq!(
                Visitor::visit_expr(&mut RejectQueries, &input),
                Err("query"),
                "{sql}"
            );
        }
    }

    #[test]
    fn postorder() {
        let input = expr("a + b");
        let mut visited = Vec::new();
        let result: Result<(), &str> = try_visit_expr(&input, &mut |expr| {
            visited.push(expr.clone());
            Ok(())
        });
        assert_eq!(result, Ok(()));
        assert_eq!(visited, vec![expr("a"), expr("b"), expr("a + b")]);
    }
}
