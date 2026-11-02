use {
    super::{Visitor, expr::walk_aggregate},
    crate::plan::{
        AggregationInputPlan, AggregationPlan, DistinctInputPlan, DistinctPlan, FilterInputPlan,
        FilterPlan, HashJoinInputPlan, HashJoinPlan, HavingPlan, InnerJoinInputPlan, InnerJoinPlan,
        JoinConditionInputPlan, JoinConditionPlan, LeftOuterJoinInputPlan, LeftOuterJoinPlan,
        LimitInputPlan, LimitPlan, NestedLoopJoinInputPlan, NestedLoopJoinPlan, OffsetInputPlan,
        OffsetPlan, OrderByExprPlan, ProjectInputPlan, ProjectPlan, ProjectionPlan, QueryPlan,
        SelectItemPlan, SelectOrderByPlan, ValuesOrderByPlan, ValuesPlan,
    },
};

pub fn walk_query<V: Visitor + ?Sized>(visitor: &mut V, query: &QueryPlan) -> Result<(), V::Error> {
    QueryWalker { visitor }.query(query)
}

struct QueryWalker<'a, V: ?Sized> {
    visitor: &'a mut V,
}

impl<V: Visitor + ?Sized> QueryWalker<'_, V> {
    fn query(&mut self, query: &QueryPlan) -> Result<(), V::Error> {
        match query {
            QueryPlan::Project(plan) => self.project(plan),
            QueryPlan::Values(plan) => self.values(plan),
            QueryPlan::SelectOrderBy(plan) => self.select_order_by(plan),
            QueryPlan::ValuesOrderBy(plan) => self.values_order_by(plan),
            QueryPlan::Distinct(plan) => self.distinct(plan),
            QueryPlan::Offset(plan) => self.offset(plan),
            QueryPlan::Limit(plan) => self.limit(plan),
        }
    }

    fn limit(&mut self, plan: &LimitPlan) -> Result<(), V::Error> {
        match &plan.input {
            LimitInputPlan::Project(plan) => self.project(plan),
            LimitInputPlan::Values(plan) => self.values(plan),
            LimitInputPlan::SelectOrderBy(plan) => self.select_order_by(plan),
            LimitInputPlan::ValuesOrderBy(plan) => self.values_order_by(plan),
            LimitInputPlan::Distinct(plan) => self.distinct(plan),
            LimitInputPlan::Offset(plan) => self.offset(plan),
        }?;
        self.visitor.visit_expr(&plan.count)
    }

    fn offset(&mut self, plan: &OffsetPlan) -> Result<(), V::Error> {
        match &plan.input {
            OffsetInputPlan::Project(plan) => self.project(plan),
            OffsetInputPlan::Values(plan) => self.values(plan),
            OffsetInputPlan::SelectOrderBy(plan) => self.select_order_by(plan),
            OffsetInputPlan::ValuesOrderBy(plan) => self.values_order_by(plan),
            OffsetInputPlan::Distinct(plan) => self.distinct(plan),
        }?;
        self.visitor.visit_expr(&plan.count)
    }

    fn distinct(&mut self, plan: &DistinctPlan) -> Result<(), V::Error> {
        match &plan.input {
            DistinctInputPlan::Project(plan) => self.project(plan),
            DistinctInputPlan::SelectOrderBy(plan) => self.select_order_by(plan),
        }
    }

    fn select_order_by(&mut self, plan: &SelectOrderByPlan) -> Result<(), V::Error> {
        self.project(&plan.input)?;
        self.order_by(&plan.exprs)
    }

    fn values_order_by(&mut self, plan: &ValuesOrderByPlan) -> Result<(), V::Error> {
        self.values(&plan.input)?;
        self.order_by(&plan.exprs)
    }

    fn order_by(&mut self, exprs: &[OrderByExprPlan]) -> Result<(), V::Error> {
        for expr in exprs {
            self.visitor.visit_expr(&expr.expr)?;
        }
        Ok(())
    }

    fn values(&mut self, ValuesPlan(rows): &ValuesPlan) -> Result<(), V::Error> {
        for expr in rows.iter().flatten() {
            self.visitor.visit_expr(expr)?;
        }
        Ok(())
    }

    fn project(&mut self, plan: &ProjectPlan) -> Result<(), V::Error> {
        match &plan.input {
            ProjectInputPlan::Source(source) => self.visitor.visit_source(source),
            ProjectInputPlan::InnerJoin(join) => self.inner_join(join),
            ProjectInputPlan::LeftOuterJoin(join) => self.left_outer_join(join),
            ProjectInputPlan::Filter(filter) => self.filter(filter),
            ProjectInputPlan::Aggregation(plan) => self.aggregation(plan),
            ProjectInputPlan::Having(plan) => self.having(plan),
        }?;
        if let ProjectionPlan::SelectItems(items) = &plan.projection {
            for item in items {
                if let SelectItemPlan::Expr { expr, .. } = item {
                    self.visitor.visit_expr(expr)?;
                }
            }
        }
        Ok(())
    }

    fn aggregation(&mut self, plan: &AggregationPlan) -> Result<(), V::Error> {
        match &plan.input {
            AggregationInputPlan::Source(source) => self.visitor.visit_source(source),
            AggregationInputPlan::InnerJoin(join) => self.inner_join(join),
            AggregationInputPlan::LeftOuterJoin(join) => self.left_outer_join(join),
            AggregationInputPlan::Filter(filter) => self.filter(filter),
        }?;
        for expr in &plan.group_by {
            self.visitor.visit_expr(expr)?;
        }
        for aggregate in &plan.aggregate_slots {
            walk_aggregate(self.visitor, aggregate)?;
        }
        Ok(())
    }

    fn filter(&mut self, plan: &FilterPlan) -> Result<(), V::Error> {
        match &plan.input {
            FilterInputPlan::Source(source) => self.visitor.visit_source(source),
            FilterInputPlan::InnerJoin(join) => self.inner_join(join),
            FilterInputPlan::LeftOuterJoin(join) => self.left_outer_join(join),
        }?;
        self.visitor.visit_expr(&plan.expr)
    }

    fn having(&mut self, plan: &HavingPlan) -> Result<(), V::Error> {
        self.aggregation(&plan.input)?;
        self.visitor.visit_expr(&plan.expr)
    }

    fn inner_join(&mut self, plan: &InnerJoinPlan) -> Result<(), V::Error> {
        match &plan.input {
            InnerJoinInputPlan::NestedLoop(plan) => self.nested_loop_join(plan),
            InnerJoinInputPlan::Hash(plan) => self.hash_join(plan),
            InnerJoinInputPlan::Condition(plan) => self.join_condition(plan),
        }
    }

    fn left_outer_join(&mut self, plan: &LeftOuterJoinPlan) -> Result<(), V::Error> {
        match &plan.input {
            LeftOuterJoinInputPlan::NestedLoop(plan) => self.nested_loop_join(plan),
            LeftOuterJoinInputPlan::Hash(plan) => self.hash_join(plan),
            LeftOuterJoinInputPlan::Condition(plan) => self.join_condition(plan),
        }
    }

    fn join_condition(&mut self, plan: &JoinConditionPlan) -> Result<(), V::Error> {
        match &plan.input {
            JoinConditionInputPlan::NestedLoop(plan) => self.nested_loop_join(plan),
            JoinConditionInputPlan::Hash(plan) => self.hash_join(plan),
        }?;
        self.visitor.visit_expr(&plan.expr)
    }

    fn nested_loop_join(&mut self, plan: &NestedLoopJoinPlan) -> Result<(), V::Error> {
        match &plan.input {
            NestedLoopJoinInputPlan::Source(source) => self.visitor.visit_source(source),
            NestedLoopJoinInputPlan::InnerJoin(join) => self.inner_join(join),
            NestedLoopJoinInputPlan::LeftOuterJoin(join) => self.left_outer_join(join),
        }?;
        self.visitor.visit_source(&plan.right)
    }

    fn hash_join(&mut self, plan: &HashJoinPlan) -> Result<(), V::Error> {
        match &plan.input {
            HashJoinInputPlan::Source(source) => self.visitor.visit_source(source),
            HashJoinInputPlan::InnerJoin(join) => self.inner_join(join),
            HashJoinInputPlan::LeftOuterJoin(join) => self.left_outer_join(join),
        }?;
        self.visitor.visit_source(&plan.right)?;
        self.visitor.visit_expr(&plan.input_key)?;
        self.visitor.visit_expr(&plan.right_key)?;
        if let Some(expr) = &plan.right_filter {
            self.visitor.visit_expr(expr)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use {
        super::QueryWalker,
        crate::{
            parse_sql::{parse, parse_expr},
            plan::{
                ExprPlan, HashJoinInputPlan, HashJoinPlan, InnerJoinInputPlan, InnerJoinPlan,
                JoinConditionInputPlan, JoinConditionPlan, LeftOuterJoinInputPlan,
                LeftOuterJoinPlan, ProjectInputPlan, ProjectPlan, ProjectionPlan, QueryPlan,
                SourcePlan, StatementPlanKind, TableAccessPlan, TableSourcePlan,
            },
            planner::{
                aggregate,
                visit::{Visitor, walk_expr, walk_source},
            },
            translate::{NO_PARAMS, translate, translate_expr},
        },
        pretty_assertions::assert_eq,
    };

    fn query(sql: &str) -> QueryPlan {
        let parsed = parse(sql).expect(sql).into_iter().next().expect(sql);
        // The call planner receives queries after aggregate planning.
        let statement = aggregate::plan(translate(&parsed).expect(sql).into());
        let StatementPlanKind::Query(query) = statement.kind else {
            panic!("expected query: {sql}");
        };
        query
    }

    fn expr(sql: &str) -> ExprPlan {
        translate_expr(&parse_expr(sql).expect(sql), NO_PARAMS)
            .expect(sql)
            .into()
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

    fn test(input: &str, expected: &[&str]) {
        let actual = query(input);
        let mut visitor = Identifiers::default();
        assert_eq!(visitor.visit_query(&actual), Ok(()), "{input}");
        assert_eq!(visitor.0, expected, "{input}");
    }

    #[test]
    fn query_inputs() {
        test("SELECT x AS r", &["x"]);
        test("VALUES (x, y), (z, w)", &["x", "y", "z", "w"]);
        test("SELECT x AS r ORDER BY y, z", &["x", "y", "z"]);
        test("VALUES (x) ORDER BY y", &["x", "y"]);
        test("SELECT DISTINCT x AS r", &["x"]);
        test("SELECT DISTINCT x AS r ORDER BY y", &["x", "y"]);

        test("SELECT x AS r LIMIT n", &["x", "n"]);
        test("VALUES (x) LIMIT n", &["x", "n"]);
        test("SELECT x AS r ORDER BY y LIMIT n", &["x", "y", "n"]);
        test("VALUES (x) ORDER BY y LIMIT n", &["x", "y", "n"]);
        test("SELECT DISTINCT x AS r LIMIT n", &["x", "n"]);
        test("SELECT x AS r OFFSET m LIMIT n", &["x", "m", "n"]);

        test("SELECT x AS r OFFSET n", &["x", "n"]);
        test("VALUES (x) OFFSET n", &["x", "n"]);
        test("SELECT x AS r ORDER BY y OFFSET n", &["x", "y", "n"]);
        test("VALUES (x) ORDER BY y OFFSET n", &["x", "y", "n"]);
        test("SELECT DISTINCT x AS r OFFSET n", &["x", "n"]);

        test("SELECT *, t.*, x AS r FROM Item t", &["x"]);
        test("SELECT x AS r FROM SERIES(n)", &["n", "x"]);
        test("SELECT x AS r FROM (VALUES (y)) v", &["y", "x"]);
        test("SELECT x AS r FROM GLUE_TABLES", &["x"]);
        test("SELECT x AS r FROM Item WHERE y", &["y", "x"]);
        test("SELECT x AS r FROM Item GROUP BY y", &["y", "x"]);
        test(
            "SELECT x AS r FROM Item WHERE y GROUP BY z HAVING w",
            &["y", "z", "w", "x"],
        );
        test(
            "SELECT COUNT(*) AS a, COUNT(x) AS b, SUM(y) AS c, MIN(z) AS d, MAX(w) AS e, AVG(v) AS f, VARIANCE(u) AS g, STDEV(t) AS h FROM Item",
            &[
                "x", "y", "z", "w", "v", "u", "t", "x", "y", "z", "w", "v", "u", "t",
            ],
        );
    }

    #[test]
    fn join_inputs() {
        test("SELECT x AS r FROM A JOIN B", &["x"]);
        test("SELECT x AS r FROM A LEFT JOIN B", &["x"]);
        test("SELECT x AS r FROM A JOIN B ON y", &["y", "x"]);
        test("SELECT x AS r FROM A LEFT JOIN B ON y", &["y", "x"]);
        test("SELECT x AS r FROM A JOIN B WHERE y", &["y", "x"]);
        test("SELECT x AS r FROM A LEFT JOIN B WHERE y", &["y", "x"]);
        test("SELECT x AS r FROM A JOIN B GROUP BY y", &["y", "x"]);
        test("SELECT x AS r FROM A LEFT JOIN B GROUP BY y", &["y", "x"]);
        test("SELECT x AS r FROM A JOIN B JOIN SERIES(y)", &["y", "x"]);
        test(
            "SELECT x AS r FROM A LEFT JOIN B JOIN SERIES(y)",
            &["y", "x"],
        );
    }

    #[derive(Default)]
    struct Expressions {
        visited: Vec<ExprPlan>,
        stop_after: Option<usize>,
    }

    impl Visitor for Expressions {
        type Error = &'static str;

        fn visit_expr(&mut self, expr: &ExprPlan) -> Result<(), Self::Error> {
            self.visited.push(expr.clone());
            if self.stop_after == Some(self.visited.len()) {
                return Err("stop");
            }
            Ok(())
        }
    }

    struct RejectSource {
        name: &'static str,
        visited: Vec<String>,
    }

    impl Visitor for RejectSource {
        type Error = &'static str;

        fn visit_expr(&mut self, _: &ExprPlan) -> Result<(), Self::Error> {
            panic!("rejected source must stop before expressions")
        }

        fn visit_source(&mut self, source: &SourcePlan) -> Result<(), Self::Error> {
            self.visited.push(source.alias_name().to_owned());
            if source.alias_name() == self.name {
                return Err("source");
            }
            walk_source(self, source)
        }
    }

    #[test]
    fn hash_join_inputs() {
        let source = SourcePlan::Table(TableSourcePlan {
            name: "Item".to_owned(),
            alias: None,
            access: TableAccessPlan::FullScan,
        });
        let hash = HashJoinPlan {
            input: HashJoinInputPlan::Source(source),
            right: SourcePlan::Table(TableSourcePlan {
                name: "Other".to_owned(),
                alias: None,
                access: TableAccessPlan::FullScan,
            }),
            input_key: expr("left_key"),
            right_key: expr("right_key"),
            right_filter: None,
        };
        let cases = [
            (
                HashJoinInputPlan::Source(hash.right.clone()),
                vec![expr("left_key"), expr("right_key"), expr("filter")],
            ),
            (
                HashJoinInputPlan::InnerJoin(Box::new(InnerJoinPlan {
                    input: InnerJoinInputPlan::Hash(hash.clone()),
                })),
                vec![
                    expr("left_key"),
                    expr("right_key"),
                    expr("left_key"),
                    expr("right_key"),
                    expr("filter"),
                ],
            ),
            (
                HashJoinInputPlan::LeftOuterJoin(Box::new(LeftOuterJoinPlan {
                    input: LeftOuterJoinInputPlan::Hash(hash.clone()),
                })),
                vec![
                    expr("left_key"),
                    expr("right_key"),
                    expr("left_key"),
                    expr("right_key"),
                    expr("filter"),
                ],
            ),
        ];
        for (input, expected) in cases {
            let plan = HashJoinPlan {
                input,
                right_filter: Some(expr("filter")),
                ..hash.clone()
            };
            let mut read = Expressions::default();
            assert_eq!(QueryWalker { visitor: &mut read }.hash_join(&plan), Ok(()));
            assert_eq!(read.visited, expected);

            for end in 0..expected.len() {
                let mut read = Expressions {
                    stop_after: Some(end + 1),
                    ..Default::default()
                };
                let result = QueryWalker { visitor: &mut read }.hash_join(&plan);
                assert_eq!(result, Err("stop"));
                assert_eq!(read.visited, expected[..=end]);
            }
        }

        for (name, expected) in [("Item", vec!["Item"]), ("Other", vec!["Item", "Other"])] {
            let mut read = RejectSource {
                name,
                visited: Vec::new(),
            };
            let result = QueryWalker { visitor: &mut read }.hash_join(&hash);
            assert_eq!(result, Err("source"));
            assert_eq!(read.visited, expected);
        }

        let condition = JoinConditionPlan {
            input: JoinConditionInputPlan::Hash(hash),
            expr: expr("condition"),
        };
        for input in [
            ProjectInputPlan::InnerJoin(Box::new(InnerJoinPlan {
                input: InnerJoinInputPlan::Condition(condition.clone()),
            })),
            ProjectInputPlan::LeftOuterJoin(Box::new(LeftOuterJoinPlan {
                input: LeftOuterJoinInputPlan::Condition(condition.clone()),
            })),
        ] {
            let query = QueryPlan::Project(ProjectPlan {
                input,
                projection: ProjectionPlan::SchemalessMap,
            });
            let expected = vec![expr("left_key"), expr("right_key"), expr("condition")];
            let mut read = Expressions::default();
            assert_eq!(Visitor::visit_query(&mut read, &query), Ok(()));
            assert_eq!(read.visited, expected);
        }
    }

    #[derive(Default)]
    struct RejectIdentifiers(Vec<String>);

    impl Visitor for RejectIdentifiers {
        type Error = &'static str;

        fn visit_expr(&mut self, expr: &ExprPlan) -> Result<(), Self::Error> {
            walk_expr(self, expr)?;
            if let ExprPlan::Identifier(name) = expr {
                self.0.push(name.clone());
                return Err("stop");
            }
            Ok(())
        }
    }

    #[test]
    fn callback_errors() {
        for sql in [
            "VALUES (stop, later)",
            "VALUES (stop) OFFSET later",
            "VALUES (stop) ORDER BY later",
            "SELECT stop AS r, later AS s FROM Item",
            "SELECT stop AS r FROM Item ORDER BY later",
            "SELECT 1 FROM Item WHERE stop",
            "SELECT 1 FROM SERIES(stop) WHERE later",
            "SELECT 1 FROM Item GROUP BY stop",
            "SELECT 1 FROM SERIES(stop) GROUP BY later",
            "SELECT 1 FROM Item GROUP BY 1 HAVING stop",
            "SELECT 1 FROM Item GROUP BY stop HAVING later",
            "SELECT SUM(stop) AS r FROM Item",
            "SELECT 1 FROM Item ORDER BY stop, later",
            "VALUES (1) ORDER BY stop, later",
            "SELECT 1 FROM Item LIMIT stop",
            "VALUES (1) OFFSET stop LIMIT later",
            "SELECT 1 FROM SERIES(stop)",
            "SELECT 1 FROM (VALUES (stop)) v",
            "SELECT 1 FROM Item JOIN Other ON stop",
            "SELECT 1 FROM Item LEFT JOIN Other ON stop",
            "SELECT 1 FROM SERIES(stop) JOIN Item",
            "SELECT 1 FROM SERIES(stop) JOIN Item ON later",
        ] {
            let query = query(sql);
            let mut read = RejectIdentifiers::default();
            assert_eq!(
                Visitor::visit_query(&mut read, &query),
                Err("stop"),
                "{sql}"
            );
            assert_eq!(read.0, vec!["stop".to_owned()], "{sql}");
        }

        for (sql, name) in [
            ("SELECT x FROM Item", "Item"),
            ("SELECT x FROM SERIES(n)", "SERIES"),
            ("SELECT x FROM (VALUES (y)) v", "v"),
            ("SELECT x FROM GLUE_TABLES", "GLUE_TABLES"),
        ] {
            let query = query(sql);
            let mut read = RejectSource {
                name,
                visited: Vec::new(),
            };
            assert_eq!(
                Visitor::visit_query(&mut read, &query),
                Err("source"),
                "{sql}"
            );
            assert_eq!(read.visited, vec![name]);
        }
    }
}
