mod aggregate;
mod function;

use {
    crate::ast::{Expr, Query},
    std::iter::once,
};

#[derive(Debug, PartialEq, Eq)]
pub enum PlanExpr<'a> {
    None,
    Identifier(&'a str),
    CompoundIdentifier { alias: &'a str, ident: &'a str },
    Expr(&'a Expr),
    TwoExprs(&'a Expr, &'a Expr),
    ThreeExprs(&'a Expr, &'a Expr, &'a Expr),
    MultiExprs(Vec<&'a Expr>),
    Query(&'a Query),
    QueryAndExpr { query: &'a Query, expr: &'a Expr },
}

impl<'a> From<&'a Expr> for PlanExpr<'a> {
    fn from(expr: &'a Expr) -> Self {
        match expr {
            Expr::Literal(_) | Expr::TypedString { .. } => PlanExpr::None,
            Expr::Identifier(ident) => PlanExpr::Identifier(ident),
            Expr::CompoundIdentifier { alias, ident } => {
                PlanExpr::CompoundIdentifier { alias, ident }
            }
            Expr::Nested(expr)
            | Expr::UnaryOp { expr, .. }
            | Expr::IsNull(expr)
            | Expr::IsNotNull(expr)
            | Expr::Interval { expr, .. } => PlanExpr::Expr(expr),
            Expr::Aggregate(aggregate) => match aggregate.as_expr() {
                Some(expr) => PlanExpr::Expr(expr),
                None => PlanExpr::None,
            },
            Expr::BinaryOp { left, right, .. } => PlanExpr::TwoExprs(left, right),
            Expr::Like { expr, pattern, .. } | Expr::ILike { expr, pattern, .. } => {
                PlanExpr::TwoExprs(expr, pattern)
            }
            Expr::Between {
                expr, low, high, ..
            } => PlanExpr::ThreeExprs(expr, low, high),
            Expr::InList { expr, list, .. } => {
                let exprs = list.iter().chain(once(expr.as_ref())).collect();

                PlanExpr::MultiExprs(exprs)
            }
            Expr::Case {
                operand,
                when_then,
                else_result,
            } => {
                let (when, then): (Vec<&Expr>, Vec<_>) =
                    when_then.iter().map(|(expr, expr2)| (expr, expr2)).unzip();

                let exprs = when
                    .into_iter()
                    .chain(then)
                    .chain(operand.iter().map(AsRef::as_ref))
                    .chain(else_result.iter().map(AsRef::as_ref))
                    .collect();

                PlanExpr::MultiExprs(exprs)
            }
            Expr::ArrayIndex { obj, indexes } => {
                let exprs = indexes.iter().chain(once(obj.as_ref())).collect();
                PlanExpr::MultiExprs(exprs)
            }
            Expr::Array { elem } => {
                let exprs = elem.iter().collect();
                PlanExpr::MultiExprs(exprs)
            }
            Expr::Function(function) => PlanExpr::MultiExprs(function.as_exprs().collect()),
            Expr::Subquery(subquery) | Expr::Exists { subquery, .. } => PlanExpr::Query(subquery),
            Expr::InSubquery {
                expr,
                subquery: query,
                ..
            } => PlanExpr::QueryAndExpr { expr, query },
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::PlanExpr,
        crate::{
            ast::{Expr, Query},
            parse_sql::{parse_expr, parse_query},
            translate::{translate_expr, translate_query},
        },
    };

    fn expr(sql: &str) -> Expr {
        let parsed = parse_expr(sql).expect(sql);

        translate_expr(&parsed).expect(sql)
    }

    fn query(sql: &str) -> Query {
        let parsed = parse_query(sql).expect(sql);

        translate_query(&parsed).expect(sql)
    }

    #[test]
    fn expr_to_plan_expr() {
        macro_rules! test {
            ($actual: expr, $expected: expr) => {
                assert_eq!(PlanExpr::from(&$actual), $expected);
            };
        }

        // PlanExpr::None
        assert_eq!(
            PlanExpr::from(&expr(r#"DATE "2022-03-09""#)),
            PlanExpr::None
        );
        assert_eq!(PlanExpr::from(&expr("100")), PlanExpr::None);
        assert_eq!(PlanExpr::from(&expr("COUNT(*)")), PlanExpr::None);

        // PlanExpr::Identifier
        let actual = expr("id");
        let expected = PlanExpr::Identifier("id");
        test!(actual, expected);

        // PlanExpr::CompoundIdentifier
        let actual = expr("Foo.id");
        let expected = PlanExpr::CompoundIdentifier {
            alias: "Foo",
            ident: "id",
        };
        test!(actual, expected);

        // PlanExpr::Expr
        let actual = expr("SUM(id)");
        let expected = expr("id");
        let expected = PlanExpr::Expr(&expected);
        test!(actual, expected);

        let actual = expr("(100)");
        let expected = expr("100");
        let expected = PlanExpr::Expr(&expected);
        test!(actual, expected);

        let actual = expr("-100");
        let expected = expr("100");
        let expected = PlanExpr::Expr(&expected);
        test!(actual, expected);

        let actual = expr("2048 IS NULL");
        let expected = expr("2048");
        let expected = PlanExpr::Expr(&expected);
        test!(actual, expected);

        let actual = expr("1989 IS NOT NULL");
        let expected = expr("1989");
        let expected = PlanExpr::Expr(&expected);
        test!(actual, expected);

        // PlanExpr::TwoExprs
        let actual = expr("100 * rate");
        let left = expr("100");
        let right = expr("rate");
        let expected = PlanExpr::TwoExprs(&left, &right);
        test!(actual, expected);

        let actual = expr("name LIKE '_foo%'");
        let target = expr("name");
        let pattern = expr("'_foo%'");
        let expected = PlanExpr::TwoExprs(&target, &pattern);
        test!(actual, expected);

        let actual = expr("name ILIKE '_foo%'");
        let target = expr("name");
        let pattern = expr("'_foo%'");
        let expected = PlanExpr::TwoExprs(&target, &pattern);
        test!(actual, expected);

        // PlanExpr::ThreeExprs
        let actual = expr("100 BETWEEN min_score AND max_score");
        let target = expr("100");
        let low = expr("min_score");
        let high = expr("max_score");
        let expected = PlanExpr::ThreeExprs(&target, &low, &high);
        test!(actual, expected);

        let actual = expr("field IN (1, 2, 3, 4, 5)");
        let expected = ["1", "2", "3", "4", "5", "field"]
            .into_iter()
            .map(expr)
            .collect::<Vec<_>>();
        let expected = PlanExpr::MultiExprs(expected.iter().collect());
        test!(actual, expected);

        let actual = expr(
            "
            CASE id
                WHEN 10 THEN col1
                WHEN 20 THEN col2
                ELSE col3
            END
        ",
        );
        let expected = ["10", "20", "col1", "col2", "id", "col3"]
            .into_iter()
            .map(expr)
            .collect::<Vec<_>>();
        let expected = PlanExpr::MultiExprs(expected.iter().collect());
        test!(actual, expected);

        let actual = expr(r#"TRIM(LEADING "x" FROM "xxx" || field)"#);
        let expected = [r#""xxx" || field"#, r#""x""#]
            .into_iter()
            .map(expr)
            .collect::<Vec<_>>();
        let expected = PlanExpr::MultiExprs(expected.iter().collect());
        test!(actual, expected);

        let actual = expr("CAST(0 AS BOOLEAN)");
        let expected = ["0"].into_iter().map(expr).collect::<Vec<_>>();
        let expected = PlanExpr::MultiExprs(expected.iter().collect());
        test!(actual, expected);

        let actual = expr(r#"EXTRACT(YEAR FROM "2000-01-01")"#);
        let expected = [r#""2000-01-01""#]
            .into_iter()
            .map(expr)
            .collect::<Vec<_>>();
        let expected = PlanExpr::MultiExprs(expected.iter().collect());
        test!(actual, expected);

        let actual = Expr::Subquery(Box::new(query("SELECT id FROM Foo")));
        let expected = query("SELECT id FROM Foo");
        let expected = PlanExpr::Query(&expected);
        test!(actual, expected);

        let actual = expr("1 IN (SELECT id FROM Foo)");
        let target = expr("1");
        let subquery = query("SELECT id FROM Foo");
        let expected = PlanExpr::QueryAndExpr {
            expr: &target,
            query: &subquery,
        };
        test!(actual, expected);

        let actual = expr(r#"["GlueSql","Rust"]"#);
        let expected = ["GlueSql", "Rust"]
            .into_iter()
            .map(expr)
            .collect::<Vec<_>>();
        let expected = PlanExpr::MultiExprs(expected.iter().collect());
        test!(actual, expected);
    }
}
