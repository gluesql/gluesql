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

pub fn walk_project_mut<V: VisitorMut + ?Sized>(
    visitor: &mut V,
    project: &mut ProjectPlan,
) -> Result<(), V::Error> {
    QueryWalkerMut { visitor }.project(project)
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

    struct MarkIdentifiers;

    impl VisitorMut for MarkIdentifiers {
        type Error = &'static str;

        fn visit_expr(&mut self, expr: &mut ExprPlan) -> Result<(), Self::Error> {
            walk_expr_mut(self, expr)?;
            if let ExprPlan::Identifier(name) = expr {
                *name = format!("__{name}__");
            }
            Ok(())
        }
    }

    fn test(actual: &str, expected: &str, name: &str) {
        let mut result = query(actual);
        assert_eq!(
            MarkIdentifiers.visit_query(&mut result),
            Ok(()),
            "[{name}] {actual}"
        );
        assert_eq!(
            result,
            query(expected),
            "\n[{name}]\nactual: {actual}\nexpected: {expected}"
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

    fn test_error(actual: &str, name: &str) {
        let mut input = query(actual);
        let mut visitor = RejectIdentifiers::default();
        let result = visitor.visit_query(&mut input);
        assert_eq!(result, Err("stop"), "[{name}] {actual}");
        assert_eq!(visitor.0, ["stop"], "[{name}] {actual}");
    }

    #[test]
    fn limit() {
        let actual = "SELECT x AS r LIMIT n";
        let expected = "SELECT __x__ AS r LIMIT __n__";
        test(actual, expected, "project");

        let actual = "VALUES (x) LIMIT n";
        let expected = "VALUES (__x__) LIMIT __n__";
        test(actual, expected, "values");

        let actual = "SELECT x AS r ORDER BY y LIMIT n";
        let expected = "SELECT __x__ AS r ORDER BY __y__ LIMIT __n__";
        test(actual, expected, "select order by");

        let actual = "VALUES (x) ORDER BY y LIMIT n";
        let expected = "VALUES (__x__) ORDER BY __y__ LIMIT __n__";
        test(actual, expected, "values order by");

        let actual = "SELECT DISTINCT x AS r LIMIT n";
        let expected = "SELECT DISTINCT __x__ AS r LIMIT __n__";
        test(actual, expected, "distinct");

        let actual = "SELECT x AS r OFFSET m LIMIT n";
        let expected = "SELECT __x__ AS r OFFSET __m__ LIMIT __n__";
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
        let expected = "SELECT __x__ AS r OFFSET __n__";
        test(actual, expected, "project");

        let actual = "VALUES (x) OFFSET n";
        let expected = "VALUES (__x__) OFFSET __n__";
        test(actual, expected, "values");

        let actual = "SELECT x AS r ORDER BY y OFFSET n";
        let expected = "SELECT __x__ AS r ORDER BY __y__ OFFSET __n__";
        test(actual, expected, "select order by");

        let actual = "VALUES (x) ORDER BY y OFFSET n";
        let expected = "VALUES (__x__) ORDER BY __y__ OFFSET __n__";
        test(actual, expected, "values order by");

        let actual = "SELECT DISTINCT x AS r OFFSET n";
        let expected = "SELECT DISTINCT __x__ AS r OFFSET __n__";
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
        let expected = "SELECT DISTINCT __x__ AS r";
        test(actual, expected, "project");

        let actual = "SELECT DISTINCT x AS r ORDER BY y";
        let expected = "SELECT DISTINCT __x__ AS r ORDER BY __y__";
        test(actual, expected, "select order by");
    }

    #[test]
    fn select_order_by() {
        let actual = "SELECT x AS r ORDER BY y, z";
        let expected = "SELECT __x__ AS r ORDER BY __y__, __z__";
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
        let expected = "VALUES (__x__) ORDER BY __y__";
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
        let expected = "VALUES (__x__, __y__), (__z__, __w__)";
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
        let expected = "SELECT __x__ AS r";
        test(actual, expected, "expression");

        let actual = "SELECT *, t.*, x AS r FROM Item t";
        let expected = "SELECT *, t.*, __x__ AS r FROM Item t";
        test(actual, expected, "wildcards and expression");

        let actual = "SELECT x AS r FROM SERIES(n)";
        let expected = "SELECT __x__ AS r FROM SERIES(__n__)";
        test(actual, expected, "series");

        let actual = "SELECT x AS r FROM (VALUES (y)) v";
        let expected = "SELECT __x__ AS r FROM (VALUES (__y__)) v";
        test(actual, expected, "derived query");

        let actual = "SELECT x AS r FROM GLUE_TABLES";
        let expected = "SELECT __x__ AS r FROM GLUE_TABLES";
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
            let mut input = query(actual);
            let mut visitor = RejectSource {
                name: expected,
                visited: Vec::new(),
            };
            assert_eq!(
                visitor.visit_query(&mut input),
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
        let expected = "SELECT __x__ AS r FROM Item GROUP BY __y__";
        test(actual, expected, "source");

        let actual = "SELECT x AS r FROM A JOIN B GROUP BY y";
        let expected = "SELECT __x__ AS r FROM A JOIN B GROUP BY __y__";
        test(actual, expected, "inner join");

        let actual = "SELECT x AS r FROM A LEFT JOIN B GROUP BY y";
        let expected = "SELECT __x__ AS r FROM A LEFT JOIN B GROUP BY __y__";
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
        let expected = "
            SELECT
                COUNT(*) AS a,
                COUNT(__x__) AS b,
                SUM(__y__) AS c,
                MIN(__z__) AS d,
                MAX(__w__) AS e,
                AVG(__v__) AS f,
                VARIANCE(__u__) AS g,
                STDEV(__t__) AS h
            FROM Item
        ";
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
        let expected = "SELECT __x__ AS r FROM Item WHERE __y__";
        test(actual, expected, "source");

        let actual = "SELECT x AS r FROM A JOIN B WHERE y";
        let expected = "SELECT __x__ AS r FROM A JOIN B WHERE __y__";
        test(actual, expected, "inner join");

        let actual = "SELECT x AS r FROM A LEFT JOIN B WHERE y";
        let expected = "SELECT __x__ AS r FROM A LEFT JOIN B WHERE __y__";
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
        let expected = "SELECT __x__ AS r FROM Item WHERE __y__ GROUP BY __z__ HAVING __w__";
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
        let expected = "SELECT __x__ AS r FROM A JOIN B";
        test(actual, expected, "nested loop");
    }

    #[test]
    fn left_outer_join() {
        let actual = "SELECT x AS r FROM A LEFT JOIN B";
        let expected = "SELECT __x__ AS r FROM A LEFT JOIN B";
        test(actual, expected, "nested loop");
    }

    #[test]
    fn join_condition() {
        let actual = "SELECT x AS r FROM A JOIN B ON y";
        let expected = "SELECT __x__ AS r FROM A JOIN B ON __y__";
        test(actual, expected, "inner join");

        let actual = "SELECT x AS r FROM A LEFT JOIN B ON y";
        let expected = "SELECT __x__ AS r FROM A LEFT JOIN B ON __y__";
        test(actual, expected, "left outer join");
    }

    #[test]
    fn join_condition_hash() {
        let test = |mut actual: QueryPlan, expected: QueryPlan, name: &str| {
            assert_eq!(MarkIdentifiers.visit_query(&mut actual), Ok(()), "{name}");
            assert_eq!(actual, expected, "{name}");
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
        let expected_condition = JoinConditionPlan {
            input: JoinConditionInputPlan::Hash(HashJoinPlan {
                input_key: expr("__left_key__"),
                right_key: expr("__right_key__"),
                ..hash
            }),
            expr: expr("__condition__"),
        };

        let actual = QueryPlan::Project(ProjectPlan {
            input: ProjectInputPlan::InnerJoin(Box::new(InnerJoinPlan {
                input: InnerJoinInputPlan::Condition(condition.clone()),
            })),
            projection: ProjectionPlan::SchemalessMap,
        });
        let expected = QueryPlan::Project(ProjectPlan {
            input: ProjectInputPlan::InnerJoin(Box::new(InnerJoinPlan {
                input: InnerJoinInputPlan::Condition(expected_condition.clone()),
            })),
            projection: ProjectionPlan::SchemalessMap,
        });
        test(actual, expected, "inner join");

        let actual = QueryPlan::Project(ProjectPlan {
            input: ProjectInputPlan::LeftOuterJoin(Box::new(LeftOuterJoinPlan {
                input: LeftOuterJoinInputPlan::Condition(condition.clone()),
            })),
            projection: ProjectionPlan::SchemalessMap,
        });
        let expected = QueryPlan::Project(ProjectPlan {
            input: ProjectInputPlan::LeftOuterJoin(Box::new(LeftOuterJoinPlan {
                input: LeftOuterJoinInputPlan::Condition(expected_condition.clone()),
            })),
            projection: ProjectionPlan::SchemalessMap,
        });
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
        let expected = "SELECT __x__ AS r FROM A JOIN B JOIN SERIES(__y__)";
        test(actual, expected, "inner join input");

        let actual = "SELECT x AS r FROM A LEFT JOIN B JOIN SERIES(y)";
        let expected = "SELECT __x__ AS r FROM A LEFT JOIN B JOIN SERIES(__y__)";
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
        let test = |mut actual: HashJoinPlan, expected: HashJoinPlan, name: &str| {
            let result = QueryWalkerMut {
                visitor: &mut MarkIdentifiers,
            }
            .hash_join(&mut actual);
            assert_eq!(result, Ok(()), "{name}");
            assert_eq!(actual, expected, "{name}");
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

        let expected_hash = HashJoinPlan {
            input_key: expr("__left_key__"),
            right_key: expr("__right_key__"),
            ..hash.clone()
        };

        let actual = hash.clone();
        let expected = expected_hash.clone();
        test(actual, expected, "source without filter");

        let actual = HashJoinPlan {
            right_filter: Some(expr("filter")),
            ..hash.clone()
        };
        let expected = HashJoinPlan {
            right_filter: Some(expr("__filter__")),
            ..expected_hash.clone()
        };
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
        let expected = HashJoinPlan {
            input: HashJoinInputPlan::InnerJoin(Box::new(InnerJoinPlan {
                input: InnerJoinInputPlan::Hash(expected_hash.clone()),
            })),
            input_key: expr("__outer_left__"),
            right_key: expr("__outer_right__"),
            right_filter: Some(expr("__filter__")),
            ..expected_hash.clone()
        };
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
        let expected = HashJoinPlan {
            input: HashJoinInputPlan::LeftOuterJoin(Box::new(LeftOuterJoinPlan {
                input: LeftOuterJoinInputPlan::Hash(expected_hash.clone()),
            })),
            input_key: expr("__outer_left__"),
            right_key: expr("__outer_right__"),
            right_filter: Some(expr("__filter__")),
            ..expected_hash.clone()
        };
        test(actual, expected, "left outer join input");
    }

    #[test]
    fn hash_join_errors() {
        let test = |input: HashJoinPlan, expected: &[ExprPlan], name: &str| {
            for end in 0..expected.len() {
                let mut actual = input.clone();
                let mut visitor = Expressions {
                    stop_after: Some(end + 1),
                    ..Default::default()
                };
                let result = QueryWalkerMut {
                    visitor: &mut visitor,
                }
                .hash_join(&mut actual);
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
            let mut actual = hash.clone();
            let mut visitor = RejectSource {
                name: rejected,
                visited: Vec::new(),
            };
            let result = QueryWalkerMut {
                visitor: &mut visitor,
            }
            .hash_join(&mut actual);
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
