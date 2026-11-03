mod aggregate;
mod function;

pub(super) use aggregate::walk_aggregate_mut;

use {
    super::VisitorMut,
    crate::plan::{ExprPlan, QueryPlan},
    function::walk_function_mut,
    std::convert::Infallible,
};

pub fn walk_expr_mut<V: VisitorMut + ?Sized>(
    visitor: &mut V,
    expr: &mut ExprPlan,
) -> Result<(), V::Error> {
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
            walk_function_mut(visitor, func)?;
        }
        ExprPlan::Aggregate(aggr) => {
            walk_aggregate_mut(visitor, aggr)?;
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

impl<F, E> VisitorMut for ExprVisitor<'_, F>
where
    F: FnMut(&mut ExprPlan) -> Result<(), E>,
{
    type Error = E;

    fn visit_expr(&mut self, expr: &mut ExprPlan) -> Result<(), E> {
        walk_expr_mut(self, expr)?;
        (self.callback)(expr)
    }

    fn visit_query(&mut self, _: &mut QueryPlan) -> Result<(), E> {
        Ok(())
    }
}

// Expression-only adapters preserve postorder callbacks and stop at query boundaries.
pub fn visit_mut_expr<F>(expr: &mut ExprPlan, f: &mut F)
where
    F: FnMut(&mut ExprPlan),
{
    let result = try_visit_mut_expr(expr, &mut |expr| {
        f(expr);
        Ok::<(), Infallible>(())
    });
    match result {
        Ok(()) => {}
        Err(never) => match never {},
    }
}

pub fn try_visit_mut_expr<F, E>(expr: &mut ExprPlan, f: &mut F) -> Result<(), E>
where
    F: FnMut(&mut ExprPlan) -> Result<(), E>,
{
    VisitorMut::visit_expr(&mut ExprVisitor { callback: f }, expr)
}

#[cfg(test)]
mod tests {
    use {
        super::{VisitorMut, try_visit_mut_expr, visit_mut_expr, walk_expr_mut},
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
    fn visit_mut_expr_variants() {
        test("id", "__id__");
        test("t.id", "t.id");
        test("id IS NULL", "__id__ IS NULL");
        test("id IS NOT NULL", "__id__ IS NOT NULL");
        test("id IN (a, b, c)", "__id__ IN (__a__, __b__, __c__)");
        test("id IN (SELECT 1)", "__id__ IN (SELECT 1)");
        test(
            "id BETWEEN low AND high",
            "__id__ BETWEEN __low__ AND __high__",
        );
        test("name LIKE pattern", "__name__ LIKE __pattern__");
        test("name ILIKE pattern", "__name__ ILIKE __pattern__");
        test("name ~* pattern", "__name__ ~* __pattern__");
        test("a + b", "__a__ + __b__");
        test("-x", "-__x__");
        test("(id)", "(__id__)");
        test("123", "123");
        test("TRUE", "TRUE");
        test("NULL", "NULL");
        test("INT '123'", "INT '123'");
        test("ABS(x)", "ABS(__x__)");
        test("SUM(x)", "SUM(__x__)");
        test("EXISTS(SELECT 1)", "EXISTS(SELECT 1)");
        test("(SELECT x)", "(SELECT x)");
        test(
            "CASE WHEN a THEN b ELSE c END",
            "CASE WHEN __a__ THEN __b__ ELSE __c__ END",
        );
        test(
            "CASE x WHEN 1 THEN a ELSE b END",
            "CASE __x__ WHEN 1 THEN __a__ ELSE __b__ END",
        );
        test("CASE WHEN a THEN b END", "CASE WHEN __a__ THEN __b__ END");
        test("arr[idx]", "__arr__[__idx__]");
        test("INTERVAL x DAY", "INTERVAL __x__ DAY");
        test("[a, b, c]", "[__a__, __b__, __c__]");
    }

    #[test]
    fn try_visit_mut_expr_children_short_circuit_after_error() {
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
                let mut expr = ExprPlan::from(translate_expr(&parsed, NO_PARAMS).expect(sql));
                let mut visited = Vec::new();
                let actual = try_visit_mut_expr(&mut expr, &mut |expr| {
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

    impl VisitorMut for Identifiers {
        type Error = &'static str;

        fn visit_expr(&mut self, expr: &mut ExprPlan) -> Result<(), Self::Error> {
            walk_expr_mut(self, expr)?;
            if let ExprPlan::Identifier(name) = expr {
                self.0.push(name.clone());
                *name = format!("__{name}__");
            }
            Ok(())
        }
    }

    #[test]
    fn subqueries() {
        let test = |actual: &str, expected: &str, expected_identifiers: &[&str], name: &str| {
            let mut result = expr(actual);
            let mut visitor = Identifiers::default();
            assert_eq!(visitor.visit_expr(&mut result), Ok(()), "[{name}] {actual}");
            assert_eq!(visitor.0, expected_identifiers, "[{name}] {actual}");
            assert_eq!(
                result,
                expr(expected),
                "\n[{name}]\nactual: {actual}\nexpected: {expected}"
            );
        };

        let actual = "(SELECT x AS r)";
        let expected = "(SELECT __x__ AS r)";
        let expected_identifiers = &["x"];
        test(actual, expected, expected_identifiers, "scalar subquery");

        let actual = "EXISTS(SELECT x AS r FROM SERIES(n))";
        let expected = "EXISTS(SELECT __x__ AS r FROM SERIES(__n__))";
        let expected_identifiers = &["n", "x"];
        test(actual, expected, expected_identifiers, "exists");

        let actual = "x IN (SELECT y AS r FROM (VALUES (z)) v)";
        let expected = "__x__ IN (SELECT __y__ AS r FROM (VALUES (__z__)) v)";
        let expected_identifiers = &["x", "z", "y"];
        test(actual, expected, expected_identifiers, "in subquery");

        let actual = "ABS((SELECT x AS r FROM SERIES((SELECT n AS s))))";
        let expected = "ABS((SELECT __x__ AS r FROM SERIES((SELECT __n__ AS s))))";
        let expected_identifiers = &["n", "x"];
        test(
            actual,
            expected,
            expected_identifiers,
            "nested subqueries in function",
        );

        let actual = "SUM((SELECT x AS r))";
        let expected = "SUM((SELECT __x__ AS r))";
        let expected_identifiers = &["x"];
        test(actual, expected, expected_identifiers, "aggregate argument");
    }

    #[test]
    fn expression_only_subqueries() {
        let actual = "(SELECT x AS r)";
        let expected = "(SELECT x AS r)";
        test(actual, expected);

        let actual = "EXISTS(SELECT x AS r FROM SERIES(n))";
        let expected = "EXISTS(SELECT x AS r FROM SERIES(n))";
        test(actual, expected);

        let actual = "x IN (SELECT y AS r FROM (VALUES (z)) v)";
        let expected = "__x__ IN (SELECT y AS r FROM (VALUES (z)) v)";
        test(actual, expected);

        let actual = "ABS((SELECT x AS r FROM SERIES((SELECT n AS s))))";
        let expected = "ABS((SELECT x AS r FROM SERIES((SELECT n AS s))))";
        test(actual, expected);

        let actual = "SUM((SELECT x AS r))";
        let expected = "SUM((SELECT x AS r))";
        test(actual, expected);
    }

    #[test]
    fn subquery_errors() {
        struct RejectQueries;

        impl VisitorMut for RejectQueries {
            type Error = &'static str;

            fn visit_query(&mut self, _: &mut QueryPlan) -> Result<(), Self::Error> {
                Err("query")
            }
        }

        let test = |actual: &str, expected: Result<(), &str>, name: &str| {
            let mut input = expr(actual);
            let result = RejectQueries.visit_expr(&mut input);
            assert_eq!(result, expected, "[{name}] {actual}");
        };

        let actual = "(SELECT x)";
        let expected = Err("query");
        test(actual, expected, "scalar subquery");

        let actual = "EXISTS(SELECT x)";
        let expected = Err("query");
        test(actual, expected, "exists");

        let actual = "x IN (SELECT y)";
        let expected = Err("query");
        test(actual, expected, "in subquery");
    }

    #[test]
    fn postorder_and_replacement() {
        let mut input = expr("a + b");
        let mut visited = Vec::new();
        visit_mut_expr(&mut input, &mut |node| {
            visited.push(node.clone());
            if matches!(node, ExprPlan::Identifier(name) if name == "a") {
                *node = expr("(x + y)");
            }
        });
        assert_eq!(visited, vec![expr("a"), expr("b"), expr("(x + y) + b")]);
        assert_eq!(input, expr("(x + y) + b"));
    }
}
