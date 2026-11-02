use {
    super::{VisitorMut, expr::walk_aggregate_mut},
    crate::plan::{
        AggregationInputPlan, AggregationPlan, DistinctInputPlan, DistinctPlan, FilterInputPlan,
        FilterPlan, HashJoinInputPlan, HashJoinPlan, HavingPlan, InnerJoinInputPlan, InnerJoinPlan,
        JoinConditionInputPlan, JoinConditionPlan, LeftOuterJoinInputPlan, LeftOuterJoinPlan,
        LimitInputPlan, LimitPlan, NestedLoopJoinInputPlan, NestedLoopJoinPlan, OffsetInputPlan,
        OffsetPlan, OrderByExprPlan, ProjectInputPlan, ProjectPlan, ProjectionPlan, QueryPlan,
        SelectItemPlan, SelectOrderByPlan, ValuesOrderByPlan, ValuesPlan,
    },
};

pub fn walk_query_mut<V: VisitorMut + ?Sized>(
    visitor: &mut V,
    query: &mut QueryPlan,
) -> Result<(), V::Error> {
    QueryWalkerMut { visitor }.query(query)
}

struct QueryWalkerMut<'a, V: ?Sized> {
    visitor: &'a mut V,
}

impl<V: VisitorMut + ?Sized> QueryWalkerMut<'_, V> {
    fn query(&mut self, query: &mut QueryPlan) -> Result<(), V::Error> {
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

    fn limit(&mut self, plan: &mut LimitPlan) -> Result<(), V::Error> {
        match &mut plan.input {
            LimitInputPlan::Project(plan) => self.project(plan),
            LimitInputPlan::Values(plan) => self.values(plan),
            LimitInputPlan::SelectOrderBy(plan) => self.select_order_by(plan),
            LimitInputPlan::ValuesOrderBy(plan) => self.values_order_by(plan),
            LimitInputPlan::Distinct(plan) => self.distinct(plan),
            LimitInputPlan::Offset(plan) => self.offset(plan),
        }?;
        self.visitor.visit_expr(&mut plan.count)
    }

    fn offset(&mut self, plan: &mut OffsetPlan) -> Result<(), V::Error> {
        match &mut plan.input {
            OffsetInputPlan::Project(plan) => self.project(plan),
            OffsetInputPlan::Values(plan) => self.values(plan),
            OffsetInputPlan::SelectOrderBy(plan) => self.select_order_by(plan),
            OffsetInputPlan::ValuesOrderBy(plan) => self.values_order_by(plan),
            OffsetInputPlan::Distinct(plan) => self.distinct(plan),
        }?;
        self.visitor.visit_expr(&mut plan.count)
    }

    fn distinct(&mut self, plan: &mut DistinctPlan) -> Result<(), V::Error> {
        match &mut plan.input {
            DistinctInputPlan::Project(plan) => self.project(plan),
            DistinctInputPlan::SelectOrderBy(plan) => self.select_order_by(plan),
        }
    }

    fn select_order_by(&mut self, plan: &mut SelectOrderByPlan) -> Result<(), V::Error> {
        self.project(&mut plan.input)?;
        self.order_by(&mut plan.exprs)
    }

    fn values_order_by(&mut self, plan: &mut ValuesOrderByPlan) -> Result<(), V::Error> {
        self.values(&mut plan.input)?;
        self.order_by(&mut plan.exprs)
    }

    fn order_by(&mut self, exprs: &mut [OrderByExprPlan]) -> Result<(), V::Error> {
        for expr in exprs {
            self.visitor.visit_expr(&mut expr.expr)?;
        }
        Ok(())
    }

    fn values(&mut self, ValuesPlan(rows): &mut ValuesPlan) -> Result<(), V::Error> {
        for expr in rows.iter_mut().flatten() {
            self.visitor.visit_expr(expr)?;
        }
        Ok(())
    }

    fn project(&mut self, plan: &mut ProjectPlan) -> Result<(), V::Error> {
        match &mut plan.input {
            ProjectInputPlan::Source(source) => self.visitor.visit_source(source),
            ProjectInputPlan::InnerJoin(join) => self.inner_join(join),
            ProjectInputPlan::LeftOuterJoin(join) => self.left_outer_join(join),
            ProjectInputPlan::Filter(filter) => self.filter(filter),
            ProjectInputPlan::Aggregation(plan) => self.aggregation(plan),
            ProjectInputPlan::Having(plan) => self.having(plan),
        }?;
        if let ProjectionPlan::SelectItems(items) = &mut plan.projection {
            for item in items {
                if let SelectItemPlan::Expr { expr, .. } = item {
                    self.visitor.visit_expr(expr)?;
                }
            }
        }
        Ok(())
    }

    fn aggregation(&mut self, plan: &mut AggregationPlan) -> Result<(), V::Error> {
        match &mut plan.input {
            AggregationInputPlan::Source(source) => self.visitor.visit_source(source),
            AggregationInputPlan::InnerJoin(join) => self.inner_join(join),
            AggregationInputPlan::LeftOuterJoin(join) => self.left_outer_join(join),
            AggregationInputPlan::Filter(filter) => self.filter(filter),
        }?;
        for expr in &mut plan.group_by {
            self.visitor.visit_expr(expr)?;
        }
        for aggregate in &mut plan.aggregate_slots {
            walk_aggregate_mut(self.visitor, aggregate)?;
        }
        Ok(())
    }

    fn filter(&mut self, plan: &mut FilterPlan) -> Result<(), V::Error> {
        match &mut plan.input {
            FilterInputPlan::Source(source) => self.visitor.visit_source(source),
            FilterInputPlan::InnerJoin(join) => self.inner_join(join),
            FilterInputPlan::LeftOuterJoin(join) => self.left_outer_join(join),
        }?;
        self.visitor.visit_expr(&mut plan.expr)
    }

    fn having(&mut self, plan: &mut HavingPlan) -> Result<(), V::Error> {
        self.aggregation(&mut plan.input)?;
        self.visitor.visit_expr(&mut plan.expr)
    }

    fn inner_join(&mut self, plan: &mut InnerJoinPlan) -> Result<(), V::Error> {
        match &mut plan.input {
            InnerJoinInputPlan::NestedLoop(plan) => self.nested_loop_join(plan),
            InnerJoinInputPlan::Hash(plan) => self.hash_join(plan),
            InnerJoinInputPlan::Condition(plan) => self.join_condition(plan),
        }
    }

    fn left_outer_join(&mut self, plan: &mut LeftOuterJoinPlan) -> Result<(), V::Error> {
        match &mut plan.input {
            LeftOuterJoinInputPlan::NestedLoop(plan) => self.nested_loop_join(plan),
            LeftOuterJoinInputPlan::Hash(plan) => self.hash_join(plan),
            LeftOuterJoinInputPlan::Condition(plan) => self.join_condition(plan),
        }
    }

    fn join_condition(&mut self, plan: &mut JoinConditionPlan) -> Result<(), V::Error> {
        match &mut plan.input {
            JoinConditionInputPlan::NestedLoop(plan) => self.nested_loop_join(plan),
            JoinConditionInputPlan::Hash(plan) => self.hash_join(plan),
        }?;
        self.visitor.visit_expr(&mut plan.expr)
    }

    fn nested_loop_join(&mut self, plan: &mut NestedLoopJoinPlan) -> Result<(), V::Error> {
        match &mut plan.input {
            NestedLoopJoinInputPlan::Source(source) => self.visitor.visit_source(source),
            NestedLoopJoinInputPlan::InnerJoin(join) => self.inner_join(join),
            NestedLoopJoinInputPlan::LeftOuterJoin(join) => self.left_outer_join(join),
        }?;
        self.visitor.visit_source(&mut plan.right)
    }

    fn hash_join(&mut self, plan: &mut HashJoinPlan) -> Result<(), V::Error> {
        match &mut plan.input {
            HashJoinInputPlan::Source(source) => self.visitor.visit_source(source),
            HashJoinInputPlan::InnerJoin(join) => self.inner_join(join),
            HashJoinInputPlan::LeftOuterJoin(join) => self.left_outer_join(join),
        }?;
        self.visitor.visit_source(&mut plan.right)?;
        self.visitor.visit_expr(&mut plan.input_key)?;
        self.visitor.visit_expr(&mut plan.right_key)?;
        if let Some(expr) = &mut plan.right_filter {
            self.visitor.visit_expr(expr)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use {
        super::QueryWalkerMut,
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
                visit_mut::{VisitorMut, walk_expr_mut, walk_source_mut},
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

    struct PrefixIdentifiers;

    impl VisitorMut for PrefixIdentifiers {
        type Error = &'static str;

        fn visit_expr(&mut self, expr: &mut ExprPlan) -> Result<(), Self::Error> {
            walk_expr_mut(self, expr)?;
            if let ExprPlan::Identifier(name) = expr {
                *name = format!("_{name}");
            }
            Ok(())
        }
    }

    fn test(input: &str, expected: &str) {
        let mut actual = query(input);
        assert_eq!(
            PrefixIdentifiers.visit_query(&mut actual),
            Ok(()),
            "{input}"
        );
        assert_eq!(actual, query(expected), "{input}");
    }

    #[test]
    fn query_inputs() {
        test("SELECT x AS r", "SELECT _x AS r");
        test("VALUES (x, y), (z, w)", "VALUES (_x, _y), (_z, _w)");
        test(
            "SELECT x AS r ORDER BY y, z",
            "SELECT _x AS r ORDER BY _y, _z",
        );
        test("VALUES (x) ORDER BY y", "VALUES (_x) ORDER BY _y");
        test("SELECT DISTINCT x AS r", "SELECT DISTINCT _x AS r");
        test(
            "SELECT DISTINCT x AS r ORDER BY y",
            "SELECT DISTINCT _x AS r ORDER BY _y",
        );

        test("SELECT x AS r LIMIT n", "SELECT _x AS r LIMIT _n");
        test("VALUES (x) LIMIT n", "VALUES (_x) LIMIT _n");
        test(
            "SELECT x AS r ORDER BY y LIMIT n",
            "SELECT _x AS r ORDER BY _y LIMIT _n",
        );
        test(
            "VALUES (x) ORDER BY y LIMIT n",
            "VALUES (_x) ORDER BY _y LIMIT _n",
        );
        test(
            "SELECT DISTINCT x AS r LIMIT n",
            "SELECT DISTINCT _x AS r LIMIT _n",
        );
        test(
            "SELECT x AS r OFFSET m LIMIT n",
            "SELECT _x AS r OFFSET _m LIMIT _n",
        );

        test("SELECT x AS r OFFSET n", "SELECT _x AS r OFFSET _n");
        test("VALUES (x) OFFSET n", "VALUES (_x) OFFSET _n");
        test(
            "SELECT x AS r ORDER BY y OFFSET n",
            "SELECT _x AS r ORDER BY _y OFFSET _n",
        );
        test(
            "VALUES (x) ORDER BY y OFFSET n",
            "VALUES (_x) ORDER BY _y OFFSET _n",
        );
        test(
            "SELECT DISTINCT x AS r OFFSET n",
            "SELECT DISTINCT _x AS r OFFSET _n",
        );

        test(
            "SELECT *, t.*, x AS r FROM Item t",
            "SELECT *, t.*, _x AS r FROM Item t",
        );
        test(
            "SELECT x AS r FROM SERIES(n)",
            "SELECT _x AS r FROM SERIES(_n)",
        );
        test(
            "SELECT x AS r FROM (VALUES (y)) v",
            "SELECT _x AS r FROM (VALUES (_y)) v",
        );
        test(
            "SELECT x AS r FROM GLUE_TABLES",
            "SELECT _x AS r FROM GLUE_TABLES",
        );
        test(
            "SELECT x AS r FROM Item WHERE y",
            "SELECT _x AS r FROM Item WHERE _y",
        );
        test(
            "SELECT x AS r FROM Item GROUP BY y",
            "SELECT _x AS r FROM Item GROUP BY _y",
        );
        test(
            "SELECT x AS r FROM Item WHERE y GROUP BY z HAVING w",
            "SELECT _x AS r FROM Item WHERE _y GROUP BY _z HAVING _w",
        );
        test(
            "SELECT COUNT(*) AS a, COUNT(x) AS b, SUM(y) AS c, MIN(z) AS d, MAX(w) AS e, AVG(v) AS f, VARIANCE(u) AS g, STDEV(t) AS h FROM Item",
            "SELECT COUNT(*) AS a, COUNT(_x) AS b, SUM(_y) AS c, MIN(_z) AS d, MAX(_w) AS e, AVG(_v) AS f, VARIANCE(_u) AS g, STDEV(_t) AS h FROM Item",
        );
    }

    #[test]
    fn join_inputs() {
        test(
            "SELECT x AS r FROM A JOIN B",
            "SELECT _x AS r FROM A JOIN B",
        );
        test(
            "SELECT x AS r FROM A LEFT JOIN B",
            "SELECT _x AS r FROM A LEFT JOIN B",
        );
        test(
            "SELECT x AS r FROM A JOIN B ON y",
            "SELECT _x AS r FROM A JOIN B ON _y",
        );
        test(
            "SELECT x AS r FROM A LEFT JOIN B ON y",
            "SELECT _x AS r FROM A LEFT JOIN B ON _y",
        );
        test(
            "SELECT x AS r FROM A JOIN B WHERE y",
            "SELECT _x AS r FROM A JOIN B WHERE _y",
        );
        test(
            "SELECT x AS r FROM A LEFT JOIN B WHERE y",
            "SELECT _x AS r FROM A LEFT JOIN B WHERE _y",
        );
        test(
            "SELECT x AS r FROM A JOIN B GROUP BY y",
            "SELECT _x AS r FROM A JOIN B GROUP BY _y",
        );
        test(
            "SELECT x AS r FROM A LEFT JOIN B GROUP BY y",
            "SELECT _x AS r FROM A LEFT JOIN B GROUP BY _y",
        );
        test(
            "SELECT x AS r FROM A JOIN B JOIN SERIES(y)",
            "SELECT _x AS r FROM A JOIN B JOIN SERIES(_y)",
        );
        test(
            "SELECT x AS r FROM A LEFT JOIN B JOIN SERIES(y)",
            "SELECT _x AS r FROM A LEFT JOIN B JOIN SERIES(_y)",
        );
    }

    #[derive(Default)]
    struct Expressions {
        visited: Vec<ExprPlan>,
        stop_after: Option<usize>,
    }

    impl VisitorMut for Expressions {
        type Error = &'static str;

        fn visit_expr(&mut self, expr: &mut ExprPlan) -> Result<(), Self::Error> {
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

    impl VisitorMut for RejectSource {
        type Error = &'static str;

        fn visit_expr(&mut self, _: &mut ExprPlan) -> Result<(), Self::Error> {
            panic!("rejected source must stop before expressions")
        }

        fn visit_source(&mut self, source: &mut SourcePlan) -> Result<(), Self::Error> {
            self.visited.push(source.alias_name().to_owned());
            if source.alias_name() == self.name {
                return Err("source");
            }
            walk_source_mut(self, source)
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
            let mut plan = HashJoinPlan {
                input,
                right_filter: Some(expr("filter")),
                ..hash.clone()
            };
            let mut write = Expressions::default();
            assert_eq!(
                QueryWalkerMut {
                    visitor: &mut write
                }
                .hash_join(&mut plan),
                Ok(())
            );
            assert_eq!(write.visited, expected);

            for end in 0..expected.len() {
                let mut write = Expressions {
                    stop_after: Some(end + 1),
                    ..Default::default()
                };
                let result = QueryWalkerMut {
                    visitor: &mut write,
                }
                .hash_join(&mut plan);
                assert_eq!(result, Err("stop"));
                assert_eq!(write.visited, expected[..=end]);
            }
        }

        for (name, expected) in [("Item", vec!["Item"]), ("Other", vec!["Item", "Other"])] {
            let mut write = RejectSource {
                name,
                visited: Vec::new(),
            };
            let result = QueryWalkerMut {
                visitor: &mut write,
            }
            .hash_join(&mut hash.clone());
            assert_eq!(result, Err("source"));
            assert_eq!(write.visited, expected);
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
            let mut query = QueryPlan::Project(ProjectPlan {
                input,
                projection: ProjectionPlan::SchemalessMap,
            });
            let expected = vec![expr("left_key"), expr("right_key"), expr("condition")];
            let mut write = Expressions::default();
            assert_eq!(VisitorMut::visit_query(&mut write, &mut query), Ok(()));
            assert_eq!(write.visited, expected);
        }
    }

    #[derive(Default)]
    struct RejectIdentifiers(Vec<String>);

    impl VisitorMut for RejectIdentifiers {
        type Error = &'static str;

        fn visit_expr(&mut self, expr: &mut ExprPlan) -> Result<(), Self::Error> {
            walk_expr_mut(self, expr)?;
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
            let mut query = query(sql);
            let mut write = RejectIdentifiers::default();
            assert_eq!(
                VisitorMut::visit_query(&mut write, &mut query),
                Err("stop"),
                "{sql}"
            );
            assert_eq!(write.0, vec!["stop".to_owned()], "{sql}");
        }

        for (sql, name) in [
            ("SELECT x FROM Item", "Item"),
            ("SELECT x FROM SERIES(n)", "SERIES"),
            ("SELECT x FROM (VALUES (y)) v", "v"),
            ("SELECT x FROM GLUE_TABLES", "GLUE_TABLES"),
        ] {
            let mut query = query(sql);
            let mut write = RejectSource {
                name,
                visited: Vec::new(),
            };
            assert_eq!(
                VisitorMut::visit_query(&mut write, &mut query),
                Err("source"),
                "{sql}"
            );
            assert_eq!(write.visited, vec![name]);
        }
    }
}
