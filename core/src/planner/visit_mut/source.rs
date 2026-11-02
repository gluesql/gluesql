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

    #[derive(Default)]
    struct Expressions(Vec<ExprPlan>);

    impl VisitorMut for Expressions {
        type Error = &'static str;

        fn visit_expr(&mut self, expr: &mut ExprPlan) -> Result<(), Self::Error> {
            self.0.push(expr.clone());
            if let ExprPlan::Identifier(name) = expr {
                *name = format!("_{name}");
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
        let cases = [
            (
                TableAccessPlan::FullScan,
                TableAccessPlan::FullScan,
                Vec::new(),
            ),
            (
                TableAccessPlan::PrimaryKey { expr: expr("key") },
                TableAccessPlan::PrimaryKey { expr: expr("_key") },
                vec![expr("key")],
            ),
            (
                TableAccessPlan::Index {
                    name: "idx".to_owned(),
                    asc: None,
                    predicate: None,
                },
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
                TableAccessPlan::Index {
                    name: "idx".to_owned(),
                    asc: Some(true),
                    predicate: Some(IndexPredicatePlan {
                        operator: IndexOperator::Eq,
                        expr: expr("_key"),
                    }),
                },
                vec![expr("key")],
            ),
        ];
        for (access, expected_access, expected_exprs) in cases {
            let mut source = SourcePlan::Table(TableSourcePlan {
                name: "Item".to_owned(),
                alias: None,
                access,
            });
            let expected = SourcePlan::Table(TableSourcePlan {
                name: "Item".to_owned(),
                alias: None,
                access: expected_access,
            });
            let mut write = Expressions::default();
            assert_eq!(VisitorMut::visit_source(&mut write, &mut source), Ok(()));
            assert_eq!(write.0, expected_exprs);
            assert_eq!(source, expected);

            if !expected_exprs.is_empty() {
                assert_eq!(
                    VisitorMut::visit_source(&mut RejectExpressions, &mut source),
                    Err("expr")
                );
            }
        }
    }

    #[test]
    fn source_inputs() {
        for (input, expected, expressions) in [
            (
                "SELECT * FROM SERIES(n)",
                "SELECT * FROM SERIES(_n)",
                vec![expr("n")],
            ),
            (
                "SELECT * FROM (VALUES (x), (y)) v",
                "SELECT * FROM (VALUES (_x), (_y)) v",
                vec![expr("x"), expr("y")],
            ),
            (
                "SELECT * FROM GLUE_TABLES",
                "SELECT * FROM GLUE_TABLES",
                Vec::new(),
            ),
        ] {
            let mut actual = source(input);
            let mut write = Expressions::default();
            assert_eq!(
                VisitorMut::visit_source(&mut write, &mut actual),
                Ok(()),
                "{input}"
            );
            assert_eq!(write.0, expressions, "{input}");
            assert_eq!(actual, source(expected), "{input}");

            if !expressions.is_empty() {
                assert_eq!(
                    VisitorMut::visit_source(&mut RejectExpressions, &mut actual),
                    Err("expr"),
                    "{input}"
                );
            }
        }
    }
}
