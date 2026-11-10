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
        // Populate aggregate slots so their expressions are visited too.
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

    fn test(actual: &str, expected: &[&str], name: &str) {
        let input = query(actual);
        let mut visitor = Identifiers::default();
        assert_eq!(visitor.visit_query(&input), Ok(()), "[{name}] {actual}");
        assert_eq!(visitor.0, expected, "[{name}] {actual}");
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

    fn test_error(actual: &str, name: &str) {
        let input = query(actual);
        let mut visitor = RejectIdentifiers::default();
        let result = visitor.visit_query(&input);
        assert_eq!(result, Err("stop"), "[{name}] {actual}");
        assert_eq!(visitor.0, ["stop"], "[{name}] {actual}");
    }

    #[test]
    fn limit() {
        let actual = "SELECT x AS r LIMIT n";
        let expected = &["x", "n"];
        test(actual, expected, "project");

        let actual = "VALUES (x) LIMIT n";
        let expected = &["x", "n"];
        test(actual, expected, "values");

        let actual = "SELECT x AS r ORDER BY y LIMIT n";
        let expected = &["x", "y", "n"];
        test(actual, expected, "select order by");

        let actual = "VALUES (x) ORDER BY y LIMIT n";
        let expected = &["x", "y", "n"];
        test(actual, expected, "values order by");

        let actual = "SELECT DISTINCT x AS r LIMIT n";
        let expected = &["x", "n"];
        test(actual, expected, "distinct");

        let actual = "SELECT x AS r OFFSET m LIMIT n";
        let expected = &["x", "m", "n"];
        test(actual, expected, "offset");
    }

    #[test]
    fn limit_errors() {
        let actual = "SELECT 1 FROM Item LIMIT stop";
        test_error(actual, "count");

        let actual = "VALUES (1) OFFSET stop LIMIT later";
        test_error(actual, "input");
    }

    #[test]
    fn offset() {
        let actual = "SELECT x AS r OFFSET n";
        let expected = &["x", "n"];
        test(actual, expected, "project");

        let actual = "VALUES (x) OFFSET n";
        let expected = &["x", "n"];
        test(actual, expected, "values");

        let actual = "SELECT x AS r ORDER BY y OFFSET n";
        let expected = &["x", "y", "n"];
        test(actual, expected, "select order by");

        let actual = "VALUES (x) ORDER BY y OFFSET n";
        let expected = &["x", "y", "n"];
        test(actual, expected, "values order by");

        let actual = "SELECT DISTINCT x AS r OFFSET n";
        let expected = &["x", "n"];
        test(actual, expected, "distinct");
    }

    #[test]
    fn offset_errors() {
        let actual = "VALUES (stop) OFFSET later";
        test_error(actual, "input");
    }

    #[test]
    fn distinct() {
        let actual = "SELECT DISTINCT x AS r";
        let expected = &["x"];
        test(actual, expected, "project");

        let actual = "SELECT DISTINCT x AS r ORDER BY y";
        let expected = &["x", "y"];
        test(actual, expected, "select order by");
    }

    #[test]
    fn select_order_by() {
        let actual = "SELECT x AS r ORDER BY y, z";
        let expected = &["x", "y", "z"];
        test(actual, expected, "multiple sort expressions");
    }

    #[test]
    fn select_order_by_errors() {
        let actual = "SELECT stop AS r FROM Item ORDER BY later";
        test_error(actual, "input");

        let actual = "SELECT 1 FROM Item ORDER BY stop, later";
        test_error(actual, "sort expressions");
    }

    #[test]
    fn values_order_by() {
        let actual = "VALUES (x) ORDER BY y";
        let expected = &["x", "y"];
        test(actual, expected, "sort expression");
    }

    #[test]
    fn values_order_by_errors() {
        let actual = "VALUES (stop) ORDER BY later";
        test_error(actual, "input");

        let actual = "VALUES (1) ORDER BY stop, later";
        test_error(actual, "sort expressions");
    }

    #[test]
    fn values() {
        let actual = "VALUES (x, y), (z, w)";
        let expected = &["x", "y", "z", "w"];
        test(actual, expected, "multiple rows and columns");
    }

    #[test]
    fn values_errors() {
        let actual = "VALUES (stop, later)";
        test_error(actual, "row expressions");
    }

    #[test]
    fn project() {
        let actual = "SELECT x AS r";
        let expected = &["x"];
        test(actual, expected, "expression");

        let actual = "SELECT *, t.*, x AS r FROM Item t";
        let expected = &["x"];
        test(actual, expected, "wildcards and expression");

        let actual = "SELECT x AS r FROM SERIES(n)";
        let expected = &["n", "x"];
        test(actual, expected, "series");

        let actual = "SELECT x AS r FROM (VALUES (y)) v";
        let expected = &["y", "x"];
        test(actual, expected, "derived query");

        let actual = "SELECT x AS r FROM GLUE_TABLES";
        let expected = &["x"];
        test(actual, expected, "dictionary");
    }

    #[test]
    fn project_errors() {
        let actual = "SELECT stop AS r, later AS s FROM Item";
        test_error(actual, "projection");

        let actual = "SELECT 1 FROM SERIES(stop)";
        test_error(actual, "series input");

        let actual = "SELECT 1 FROM (VALUES (stop)) v";
        test_error(actual, "derived input");
    }

    #[test]
    fn project_source_errors() {
        let test = |actual: &str, expected: &'static str, name: &str| {
            let input = query(actual);
            let mut visitor = RejectSource {
                name: expected,
                visited: Vec::new(),
            };
            assert_eq!(
                visitor.visit_query(&input),
                Err("source"),
                "[{name}] {actual}"
            );
            assert_eq!(visitor.visited, [expected], "[{name}] {actual}");
        };

        let actual = "SELECT x FROM Item";
        let expected = "Item";
        test(actual, expected, "table");

        let actual = "SELECT x FROM SERIES(n)";
        let expected = "SERIES";
        test(actual, expected, "series");

        let actual = "SELECT x FROM (VALUES (y)) v";
        let expected = "v";
        test(actual, expected, "derived");

        let actual = "SELECT x FROM GLUE_TABLES";
        let expected = "GLUE_TABLES";
        test(actual, expected, "dictionary");
    }

    #[test]
    fn aggregation() {
        let actual = "SELECT x AS r FROM Item GROUP BY y";
        let expected = &["y", "x"];
        test(actual, expected, "source");

        let actual = "SELECT x AS r FROM A JOIN B GROUP BY y";
        let expected = &["y", "x"];
        test(actual, expected, "inner join");

        let actual = "SELECT x AS r FROM A LEFT JOIN B GROUP BY y";
        let expected = &["y", "x"];
        test(actual, expected, "left outer join");

        let actual = "
            SELECT
                COUNT(*) AS a,
                COUNT(x) AS b,
                SUM(y) AS c,
                MIN(z) AS d,
                MAX(w) AS e,
                AVG(v) AS f,
                VARIANCE(u) AS g,
                STDEV(t) AS h
            FROM Item
        ";
        let expected = &[
            "x", "y", "z", "w", "v", "u", "t", "x", "y", "z", "w", "v", "u", "t",
        ];
        test(actual, expected, "aggregate slots and projection");
    }

    #[test]
    fn aggregation_errors() {
        let actual = "SELECT 1 FROM Item GROUP BY stop";
        test_error(actual, "group by");

        let actual = "SELECT 1 FROM SERIES(stop) GROUP BY later";
        test_error(actual, "input");

        let actual = "SELECT SUM(stop) AS r FROM Item";
        test_error(actual, "aggregate slot");
    }

    #[test]
    fn filter() {
        let actual = "SELECT x AS r FROM Item WHERE y";
        let expected = &["y", "x"];
        test(actual, expected, "source");

        let actual = "SELECT x AS r FROM A JOIN B WHERE y";
        let expected = &["y", "x"];
        test(actual, expected, "inner join");

        let actual = "SELECT x AS r FROM A LEFT JOIN B WHERE y";
        let expected = &["y", "x"];
        test(actual, expected, "left outer join");
    }

    #[test]
    fn filter_errors() {
        let actual = "SELECT 1 FROM Item WHERE stop";
        test_error(actual, "predicate");

        let actual = "SELECT 1 FROM SERIES(stop) WHERE later";
        test_error(actual, "input");
    }

    #[test]
    fn having() {
        let actual = "SELECT x AS r FROM Item WHERE y GROUP BY z HAVING w";
        let expected = &["y", "z", "w", "x"];
        test(actual, expected, "filter and aggregation");
    }

    #[test]
    fn having_errors() {
        let actual = "SELECT 1 FROM Item GROUP BY 1 HAVING stop";
        test_error(actual, "predicate");

        let actual = "SELECT 1 FROM Item GROUP BY stop HAVING later";
        test_error(actual, "input");
    }

    #[test]
    fn inner_join() {
        let actual = "SELECT x AS r FROM A JOIN B";
        let expected = &["x"];
        test(actual, expected, "nested loop");
    }

    #[test]
    fn left_outer_join() {
        let actual = "SELECT x AS r FROM A LEFT JOIN B";
        let expected = &["x"];
        test(actual, expected, "nested loop");
    }

    #[test]
    fn join_condition() {
        let actual = "SELECT x AS r FROM A JOIN B ON y";
        let expected = &["y", "x"];
        test(actual, expected, "inner join");

        let actual = "SELECT x AS r FROM A LEFT JOIN B ON y";
        let expected = &["y", "x"];
        test(actual, expected, "left outer join");
    }

    #[test]
    fn join_condition_hash() {
        let test = |actual: QueryPlan, expected: &[ExprPlan], name: &str| {
            let mut visitor = Expressions::default();
            assert_eq!(visitor.visit_query(&actual), Ok(()), "{name}");
            assert_eq!(visitor.visited, expected, "{name}");
        };

        let hash = HashJoinPlan {
            input: HashJoinInputPlan::Source(SourcePlan::Table(TableSourcePlan {
                name: "Item".to_owned(),
                alias: None,
                access: TableAccessPlan::FullScan,
            })),
            right: SourcePlan::Table(TableSourcePlan {
                name: "Other".to_owned(),
                alias: None,
                access: TableAccessPlan::FullScan,
            }),
            input_key: expr("left_key"),
            right_key: expr("right_key"),
            right_filter: None,
        };

        let condition = JoinConditionPlan {
            input: JoinConditionInputPlan::Hash(hash.clone()),
            expr: expr("condition"),
        };

        let actual = QueryPlan::Project(ProjectPlan {
            input: ProjectInputPlan::InnerJoin(Box::new(InnerJoinPlan {
                input: InnerJoinInputPlan::Condition(condition.clone()),
            })),
            projection: ProjectionPlan::SchemalessMap,
        });
        let expected = &[expr("left_key"), expr("right_key"), expr("condition")];
        test(actual, expected, "inner join");

        let actual = QueryPlan::Project(ProjectPlan {
            input: ProjectInputPlan::LeftOuterJoin(Box::new(LeftOuterJoinPlan {
                input: LeftOuterJoinInputPlan::Condition(condition.clone()),
            })),
            projection: ProjectionPlan::SchemalessMap,
        });
        let expected = &[expr("left_key"), expr("right_key"), expr("condition")];
        test(actual, expected, "left outer join");
    }

    #[test]
    fn join_condition_errors() {
        let actual = "SELECT 1 FROM Item JOIN Other ON stop";
        test_error(actual, "inner join predicate");

        let actual = "SELECT 1 FROM Item LEFT JOIN Other ON stop";
        test_error(actual, "left outer join predicate");
    }

    #[test]
    fn nested_loop_join() {
        let actual = "SELECT x AS r FROM A JOIN B JOIN SERIES(y)";
        let expected = &["y", "x"];
        test(actual, expected, "inner join input");

        let actual = "SELECT x AS r FROM A LEFT JOIN B JOIN SERIES(y)";
        let expected = &["y", "x"];
        test(actual, expected, "left outer join input");
    }

    #[test]
    fn nested_loop_join_errors() {
        let actual = "SELECT 1 FROM SERIES(stop) JOIN Item";
        test_error(actual, "input");

        let actual = "SELECT 1 FROM SERIES(stop) JOIN Item ON later";
        test_error(actual, "input before condition");
    }

    #[test]
    fn hash_join() {
        let test = |actual: HashJoinPlan, expected: &[ExprPlan], name: &str| {
            let mut visitor = Expressions::default();
            let result = QueryWalker {
                visitor: &mut visitor,
            }
            .hash_join(&actual);
            assert_eq!(result, Ok(()), "{name}");
            assert_eq!(visitor.visited, expected, "{name}");
        };

        let hash = HashJoinPlan {
            input: HashJoinInputPlan::Source(SourcePlan::Table(TableSourcePlan {
                name: "Item".to_owned(),
                alias: None,
                access: TableAccessPlan::FullScan,
            })),
            right: SourcePlan::Table(TableSourcePlan {
                name: "Other".to_owned(),
                alias: None,
                access: TableAccessPlan::FullScan,
            }),
            input_key: expr("left_key"),
            right_key: expr("right_key"),
            right_filter: None,
        };

        let actual = hash.clone();
        let expected = &[expr("left_key"), expr("right_key")];
        test(actual, expected, "source without filter");

        let actual = HashJoinPlan {
            right_filter: Some(expr("filter")),
            ..hash.clone()
        };
        let expected = &[expr("left_key"), expr("right_key"), expr("filter")];
        test(actual, expected, "source with filter");

        let actual = HashJoinPlan {
            input: HashJoinInputPlan::InnerJoin(Box::new(InnerJoinPlan {
                input: InnerJoinInputPlan::Hash(hash.clone()),
            })),
            input_key: expr("outer_left"),
            right_key: expr("outer_right"),
            right_filter: Some(expr("filter")),
            ..hash.clone()
        };
        let expected = &[
            expr("left_key"),
            expr("right_key"),
            expr("outer_left"),
            expr("outer_right"),
            expr("filter"),
        ];
        test(actual, expected, "inner join input");

        let actual = HashJoinPlan {
            input: HashJoinInputPlan::LeftOuterJoin(Box::new(LeftOuterJoinPlan {
                input: LeftOuterJoinInputPlan::Hash(hash.clone()),
            })),
            input_key: expr("outer_left"),
            right_key: expr("outer_right"),
            right_filter: Some(expr("filter")),
            ..hash.clone()
        };
        let expected = &[
            expr("left_key"),
            expr("right_key"),
            expr("outer_left"),
            expr("outer_right"),
            expr("filter"),
        ];
        test(actual, expected, "left outer join input");
    }

    #[test]
    fn hash_join_errors() {
        let test = |input: HashJoinPlan, expected: &[ExprPlan], name: &str| {
            for end in 0..expected.len() {
                let actual = input.clone();
                let mut visitor = Expressions {
                    stop_after: Some(end + 1),
                    ..Default::default()
                };
                let result = QueryWalker {
                    visitor: &mut visitor,
                }
                .hash_join(&actual);
                assert_eq!(result, Err("stop"), "[{name}] child {end}");
                assert_eq!(visitor.visited, expected[..=end], "[{name}] child {end}");
            }
        };

        let hash = HashJoinPlan {
            input: HashJoinInputPlan::Source(SourcePlan::Table(TableSourcePlan {
                name: "Item".to_owned(),
                alias: None,
                access: TableAccessPlan::FullScan,
            })),
            right: SourcePlan::Table(TableSourcePlan {
                name: "Other".to_owned(),
                alias: None,
                access: TableAccessPlan::FullScan,
            }),
            input_key: expr("left_key"),
            right_key: expr("right_key"),
            right_filter: None,
        };

        let actual = HashJoinPlan {
            right_filter: Some(expr("filter")),
            ..hash.clone()
        };
        let expected = &[expr("left_key"), expr("right_key"), expr("filter")];
        test(actual, expected, "source input");

        let actual = HashJoinPlan {
            input: HashJoinInputPlan::InnerJoin(Box::new(InnerJoinPlan {
                input: InnerJoinInputPlan::Hash(hash.clone()),
            })),
            input_key: expr("outer_left"),
            right_key: expr("outer_right"),
            right_filter: Some(expr("filter")),
            ..hash.clone()
        };
        let expected = &[
            expr("left_key"),
            expr("right_key"),
            expr("outer_left"),
            expr("outer_right"),
            expr("filter"),
        ];
        test(actual, expected, "inner join input");

        let actual = HashJoinPlan {
            input: HashJoinInputPlan::LeftOuterJoin(Box::new(LeftOuterJoinPlan {
                input: LeftOuterJoinInputPlan::Hash(hash.clone()),
            })),
            input_key: expr("outer_left"),
            right_key: expr("outer_right"),
            right_filter: Some(expr("filter")),
            ..hash.clone()
        };
        let expected = &[
            expr("left_key"),
            expr("right_key"),
            expr("outer_left"),
            expr("outer_right"),
            expr("filter"),
        ];
        test(actual, expected, "left outer join input");
    }

    #[test]
    fn hash_join_source_errors() {
        let hash = HashJoinPlan {
            input: HashJoinInputPlan::Source(SourcePlan::Table(TableSourcePlan {
                name: "Item".to_owned(),
                alias: None,
                access: TableAccessPlan::FullScan,
            })),
            right: SourcePlan::Table(TableSourcePlan {
                name: "Other".to_owned(),
                alias: None,
                access: TableAccessPlan::FullScan,
            }),
            input_key: expr("left_key"),
            right_key: expr("right_key"),
            right_filter: None,
        };

        let test = |rejected: &'static str, expected: &[&str], name: &str| {
            let actual = hash.clone();
            let mut visitor = RejectSource {
                name: rejected,
                visited: Vec::new(),
            };
            let result = QueryWalker {
                visitor: &mut visitor,
            }
            .hash_join(&actual);
            assert_eq!(result, Err("source"), "{name}");
            assert_eq!(visitor.visited, expected, "{name}");
        };

        let actual = "Item";
        let expected = &["Item"];
        test(actual, expected, "input source");

        let actual = "Other";
        let expected = &["Item", "Other"];
        test(actual, expected, "right source");
    }
}
