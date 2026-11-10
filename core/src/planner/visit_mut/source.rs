use {
    super::VisitorMut,
    crate::plan::{SourcePlan, TableAccessPlan},
};

pub fn walk_source_mut<V: VisitorMut + ?Sized>(
    visitor: &mut V,
    source: &mut SourcePlan,
) -> Result<(), V::Error> {
    match source {
        SourcePlan::Table(table) => match &mut table.access {
            TableAccessPlan::PrimaryKey { expr } => visitor.visit_expr(expr)?,
            TableAccessPlan::Index {
                predicate: Some(predicate),
                ..
            } => visitor.visit_expr(&mut predicate.expr)?,
            TableAccessPlan::FullScan
            | TableAccessPlan::ProjectedScan { .. }
            | TableAccessPlan::Index {
                predicate: None, ..
            } => {}
        },
        SourcePlan::Derived(derived) => visitor.visit_query(&mut derived.query)?,
        SourcePlan::Series(series) => visitor.visit_expr(&mut series.size)?,
        SourcePlan::Dictionary(_) => {}
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use {
        super::super::VisitorMut,
        crate::{
            ast::IndexOperator,
            parse_sql::{parse, parse_expr},
            plan::{
                ExprPlan, IndexPredicatePlan, ProjectInputPlan, QueryPlan, SourcePlan,
                StatementPlan, StatementPlanKind, TableAccessPlan, TableSourcePlan,
            },
            translate::{NO_PARAMS, translate, translate_expr},
        },
        pretty_assertions::assert_eq,
    };

    fn expr(sql: &str) -> ExprPlan {
        translate_expr(&parse_expr(sql).expect(sql), NO_PARAMS)
            .expect(sql)
            .into()
    }

    fn source(sql: &str) -> SourcePlan {
        let parsed = parse(sql).expect(sql).into_iter().next().expect(sql);
        let statement = StatementPlan::from(translate(&parsed).expect(sql));
        let StatementPlanKind::Query(QueryPlan::Project(project)) = statement.kind else {
            panic!("expected SELECT: {sql}");
        };
        let ProjectInputPlan::Source(source) = project.input else {
            panic!("expected source: {sql}");
        };
        source
    }

    struct MarkIdentifiers;

    impl VisitorMut for MarkIdentifiers {
        type Error = &'static str;

        fn visit_expr(&mut self, expr: &mut ExprPlan) -> Result<(), Self::Error> {
            if let ExprPlan::Identifier(name) = expr {
                *name = format!("__{name}__");
            }
            Ok(())
        }
    }

    struct RejectExpressions;

    impl VisitorMut for RejectExpressions {
        type Error = &'static str;

        fn visit_expr(&mut self, _: &mut ExprPlan) -> Result<(), Self::Error> {
            Err("expr")
        }
    }

    #[test]
    fn table_access() {
        let test = |actual: TableAccessPlan, expected: TableAccessPlan, name: &str| {
            let mut actual = SourcePlan::Table(TableSourcePlan {
                name: "Item".to_owned(),
                alias: None,
                access: actual,
            });
            let expected = SourcePlan::Table(TableSourcePlan {
                name: "Item".to_owned(),
                alias: None,
                access: expected,
            });
            assert_eq!(MarkIdentifiers.visit_source(&mut actual), Ok(()), "{name}");
            assert_eq!(actual, expected, "{name}");
        };

        let actual = TableAccessPlan::FullScan;
        let expected = TableAccessPlan::FullScan;
        test(actual, expected, "full scan");

        let actual = TableAccessPlan::ProjectedScan {
            columns: vec!["name".to_owned()],
        };
        let expected = TableAccessPlan::ProjectedScan {
            columns: vec!["name".to_owned()],
        };
        test(actual, expected, "projected column names stay unchanged");

        let actual = TableAccessPlan::PrimaryKey { expr: expr("key") };
        let expected = TableAccessPlan::PrimaryKey {
            expr: expr("__key__"),
        };
        test(actual, expected, "primary key");

        let actual = TableAccessPlan::Index {
            name: "idx".to_owned(),
            asc: None,
            predicate: None,
        };
        let expected = TableAccessPlan::Index {
            name: "idx".to_owned(),
            asc: None,
            predicate: None,
        };
        test(actual, expected, "index without predicate");

        let actual = TableAccessPlan::Index {
            name: "idx".to_owned(),
            asc: Some(true),
            predicate: Some(IndexPredicatePlan {
                operator: IndexOperator::Eq,
                expr: expr("key"),
            }),
        };
        let expected = TableAccessPlan::Index {
            name: "idx".to_owned(),
            asc: Some(true),
            predicate: Some(IndexPredicatePlan {
                operator: IndexOperator::Eq,
                expr: expr("__key__"),
            }),
        };
        test(actual, expected, "index with predicate");
    }

    #[test]
    fn table_access_errors() {
        let test = |actual: TableAccessPlan, expected: Result<(), &str>, name: &str| {
            let mut actual = SourcePlan::Table(TableSourcePlan {
                name: "Item".to_owned(),
                alias: None,
                access: actual,
            });
            let result = RejectExpressions.visit_source(&mut actual);
            assert_eq!(result, expected, "{name}");
        };

        let actual = TableAccessPlan::PrimaryKey { expr: expr("key") };
        let expected = Err("expr");
        test(actual, expected, "primary key");

        let actual = TableAccessPlan::Index {
            name: "idx".to_owned(),
            asc: Some(true),
            predicate: Some(IndexPredicatePlan {
                operator: IndexOperator::Eq,
                expr: expr("key"),
            }),
        };
        let expected = Err("expr");
        test(actual, expected, "index with predicate");
    }

    #[test]
    fn source_variants() {
        let test = |actual: &str, expected: &str, name: &str| {
            let mut result = source(actual);
            assert_eq!(
                MarkIdentifiers.visit_source(&mut result),
                Ok(()),
                "[{name}] {actual}"
            );
            assert_eq!(
                result,
                source(expected),
                "\n[{name}]\nactual: {actual}\nexpected: {expected}"
            );
        };

        let actual = "SELECT * FROM SERIES(n)";
        let expected = "SELECT * FROM SERIES(__n__)";
        test(actual, expected, "series");

        let actual = "SELECT * FROM (VALUES (x), (y)) v";
        let expected = "SELECT * FROM (VALUES (__x__), (__y__)) v";
        test(actual, expected, "derived query");

        let actual = "SELECT * FROM GLUE_TABLES";
        let expected = "SELECT * FROM GLUE_TABLES";
        test(actual, expected, "dictionary");
    }

    #[test]
    fn source_errors() {
        let test = |actual: &str, expected: Result<(), &str>, name: &str| {
            let mut input = source(actual);
            let result = RejectExpressions.visit_source(&mut input);
            assert_eq!(result, expected, "[{name}] {actual}");
        };

        let actual = "SELECT * FROM SERIES(n)";
        let expected = Err("expr");
        test(actual, expected, "series");

        let actual = "SELECT * FROM (VALUES (x), (y)) v";
        let expected = Err("expr");
        test(actual, expected, "derived query");
    }
}
