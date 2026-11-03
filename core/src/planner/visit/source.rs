use {
    super::Visitor,
    crate::plan::{SourcePlan, TableAccessPlan},
};

pub fn walk_source<V: Visitor + ?Sized>(
    visitor: &mut V,
    source: &SourcePlan,
) -> Result<(), V::Error> {
    match source {
        SourcePlan::Table(table) => match &table.access {
            TableAccessPlan::PrimaryKey { expr } => visitor.visit_expr(expr)?,
            TableAccessPlan::Index {
                predicate: Some(predicate),
                ..
            } => visitor.visit_expr(&predicate.expr)?,
            TableAccessPlan::FullScan
            | TableAccessPlan::Index {
                predicate: None, ..
            } => {}
        },
        SourcePlan::Derived(derived) => visitor.visit_query(&derived.query)?,
        SourcePlan::Series(series) => visitor.visit_expr(&series.size)?,
        SourcePlan::Dictionary(_) => {}
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use {
        super::super::Visitor,
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

    #[derive(Default)]
    struct Expressions(Vec<ExprPlan>);

    impl Visitor for Expressions {
        type Error = &'static str;

        fn visit_expr(&mut self, expr: &ExprPlan) -> Result<(), Self::Error> {
            self.0.push(expr.clone());
            Ok(())
        }
    }

    struct RejectExpressions;

    impl Visitor for RejectExpressions {
        type Error = &'static str;

        fn visit_expr(&mut self, _: &ExprPlan) -> Result<(), Self::Error> {
            Err("expr")
        }
    }

    #[test]
    fn table_access() {
        let test = |actual: TableAccessPlan, expected: &[ExprPlan], name: &str| {
            let actual = SourcePlan::Table(TableSourcePlan {
                name: "Item".to_owned(),
                alias: None,
                access: actual,
            });
            let mut visitor = Expressions::default();
            assert_eq!(visitor.visit_source(&actual), Ok(()), "{name}");
            assert_eq!(visitor.0, expected, "{name}");
        };

        let actual = TableAccessPlan::FullScan;
        let expected = &[];
        test(actual, expected, "full scan");

        let actual = TableAccessPlan::PrimaryKey { expr: expr("key") };
        let expected = &[expr("key")];
        test(actual, expected, "primary key");

        let actual = TableAccessPlan::Index {
            name: "idx".to_owned(),
            asc: None,
            predicate: None,
        };
        let expected = &[];
        test(actual, expected, "index without predicate");

        let actual = TableAccessPlan::Index {
            name: "idx".to_owned(),
            asc: Some(true),
            predicate: Some(IndexPredicatePlan {
                operator: IndexOperator::Eq,
                expr: expr("key"),
            }),
        };
        let expected = &[expr("key")];
        test(actual, expected, "index with predicate");
    }

    #[test]
    fn table_access_errors() {
        let test = |actual: TableAccessPlan, expected: Result<(), &str>, name: &str| {
            let actual = SourcePlan::Table(TableSourcePlan {
                name: "Item".to_owned(),
                alias: None,
                access: actual,
            });
            let result = RejectExpressions.visit_source(&actual);
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
        let test = |actual: &str, expected: &[ExprPlan], name: &str| {
            let input = source(actual);
            let mut visitor = Expressions::default();
            assert_eq!(visitor.visit_source(&input), Ok(()), "[{name}] {actual}");
            assert_eq!(visitor.0, expected, "[{name}] {actual}");
        };

        let actual = "SELECT * FROM SERIES(n)";
        let expected = &[expr("n")];
        test(actual, expected, "series");

        let actual = "SELECT * FROM (VALUES (x), (y)) v";
        let expected = &[expr("x"), expr("y")];
        test(actual, expected, "derived query");

        let actual = "SELECT * FROM GLUE_TABLES";
        let expected = &[];
        test(actual, expected, "dictionary");
    }

    #[test]
    fn source_errors() {
        let test = |actual: &str, expected: Result<(), &str>, name: &str| {
            let input = source(actual);
            let result = RejectExpressions.visit_source(&input);
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
