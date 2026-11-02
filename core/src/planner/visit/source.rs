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
        let cases = [
            (TableAccessPlan::FullScan, Vec::new()),
            (
                TableAccessPlan::PrimaryKey { expr: expr("key") },
                vec![expr("key")],
            ),
            (
                TableAccessPlan::Index {
                    name: "idx".to_owned(),
                    asc: None,
                    predicate: None,
                },
                Vec::new(),
            ),
            (
                TableAccessPlan::Index {
                    name: "idx".to_owned(),
                    asc: Some(true),
                    predicate: Some(IndexPredicatePlan {
                        operator: IndexOperator::Eq,
                        expr: expr("key"),
                    }),
                },
                vec![expr("key")],
            ),
        ];
        for (access, expected_exprs) in cases {
            let source = SourcePlan::Table(TableSourcePlan {
                name: "Item".to_owned(),
                alias: None,
                access,
            });
            let mut read = Expressions::default();
            assert_eq!(Visitor::visit_source(&mut read, &source), Ok(()));
            assert_eq!(read.0, expected_exprs);

            if !expected_exprs.is_empty() {
                assert_eq!(
                    Visitor::visit_source(&mut RejectExpressions, &source),
                    Err("expr")
                );
            }
        }
    }

    #[test]
    fn source_inputs() {
        for (input, expressions) in [
            ("SELECT * FROM SERIES(n)", vec![expr("n")]),
            (
                "SELECT * FROM (VALUES (x), (y)) v",
                vec![expr("x"), expr("y")],
            ),
            ("SELECT * FROM GLUE_TABLES", Vec::new()),
        ] {
            let actual = source(input);
            let mut read = Expressions::default();
            assert_eq!(Visitor::visit_source(&mut read, &actual), Ok(()), "{input}");
            assert_eq!(read.0, expressions, "{input}");

            if !expressions.is_empty() {
                assert_eq!(
                    Visitor::visit_source(&mut RejectExpressions, &actual),
                    Err("expr"),
                    "{input}"
                );
            }
        }
    }
}
