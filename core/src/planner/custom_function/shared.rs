mod validate_function_definition;

pub(super) use validate_function_definition::validate as validate_function_definition;

use crate::{
    plan::{
        AggregateFunctionPlan, AggregationInputPlan, AggregationPlan, CountArgExprPlan,
        DistinctInputPlan, DistinctPlan, ExprPlan, FilterInputPlan, FilterPlan, HashJoinInputPlan,
        HashJoinPlan, HavingPlan, InnerJoinInputPlan, InnerJoinPlan, JoinConditionInputPlan,
        JoinConditionPlan, LeftOuterJoinInputPlan, LeftOuterJoinPlan, LimitInputPlan, LimitPlan,
        NestedLoopJoinInputPlan, NestedLoopJoinPlan, OffsetInputPlan, OffsetPlan, OrderByExprPlan,
        ProjectInputPlan, ProjectPlan, ProjectionPlan, QueryPlan, SelectItemPlan,
        SelectOrderByPlan, SourcePlan, TableAccessPlan, ValuesOrderByPlan, ValuesPlan,
    },
    result::Result,
};

// Shared by definition validation and call binding. Expression traversal remains
// explicit: the expression visitor does not enter queries on its own.
pub(super) fn try_visit_mut_query<F, G>(
    query: &mut QueryPlan,
    expr: &mut F,
    source: &mut G,
) -> Result<()>
where
    F: FnMut(&mut ExprPlan) -> Result<()>,
    G: FnMut(&SourcePlan) -> Result<()>,
{
    QueryVisitor { expr, source }.query(query)
}

struct QueryVisitor<'a, F, G> {
    expr: &'a mut F,
    source: &'a mut G,
}

impl<F, G> QueryVisitor<'_, F, G>
where
    F: FnMut(&mut ExprPlan) -> Result<()>,
    G: FnMut(&SourcePlan) -> Result<()>,
{
    fn query(&mut self, query: &mut QueryPlan) -> Result<()> {
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

    fn limit(&mut self, plan: &mut LimitPlan) -> Result<()> {
        match &mut plan.input {
            LimitInputPlan::Project(plan) => self.project(plan),
            LimitInputPlan::Values(plan) => self.values(plan),
            LimitInputPlan::SelectOrderBy(plan) => self.select_order_by(plan),
            LimitInputPlan::ValuesOrderBy(plan) => self.values_order_by(plan),
            LimitInputPlan::Distinct(plan) => self.distinct(plan),
            LimitInputPlan::Offset(plan) => self.offset(plan),
        }?;
        (self.expr)(&mut plan.count)
    }

    fn offset(&mut self, plan: &mut OffsetPlan) -> Result<()> {
        match &mut plan.input {
            OffsetInputPlan::Project(plan) => self.project(plan),
            OffsetInputPlan::Values(plan) => self.values(plan),
            OffsetInputPlan::SelectOrderBy(plan) => self.select_order_by(plan),
            OffsetInputPlan::ValuesOrderBy(plan) => self.values_order_by(plan),
            OffsetInputPlan::Distinct(plan) => self.distinct(plan),
        }?;
        (self.expr)(&mut plan.count)
    }

    fn distinct(&mut self, plan: &mut DistinctPlan) -> Result<()> {
        match &mut plan.input {
            DistinctInputPlan::Project(plan) => self.project(plan),
            DistinctInputPlan::SelectOrderBy(plan) => self.select_order_by(plan),
        }
    }

    fn select_order_by(&mut self, plan: &mut SelectOrderByPlan) -> Result<()> {
        self.project(&mut plan.input)?;
        self.order_by(&mut plan.exprs)
    }

    fn values_order_by(&mut self, plan: &mut ValuesOrderByPlan) -> Result<()> {
        self.values(&mut plan.input)?;
        self.order_by(&mut plan.exprs)
    }

    fn order_by(&mut self, exprs: &mut [OrderByExprPlan]) -> Result<()> {
        for expr in exprs {
            (self.expr)(&mut expr.expr)?;
        }
        Ok(())
    }

    fn values(&mut self, ValuesPlan(rows): &mut ValuesPlan) -> Result<()> {
        for expr in rows.iter_mut().flatten() {
            (self.expr)(expr)?;
        }
        Ok(())
    }

    fn project(&mut self, plan: &mut ProjectPlan) -> Result<()> {
        match &mut plan.input {
            ProjectInputPlan::Source(source) => self.source(source),
            ProjectInputPlan::InnerJoin(join) => self.inner_join(join),
            ProjectInputPlan::LeftOuterJoin(join) => self.left_outer_join(join),
            ProjectInputPlan::Filter(filter) => self.filter(filter),
            ProjectInputPlan::Aggregation(plan) => self.aggregation(plan),
            ProjectInputPlan::Having(plan) => self.having(plan),
        }?;
        if let ProjectionPlan::SelectItems(items) = &mut plan.projection {
            for item in items {
                if let SelectItemPlan::Expr { expr, .. } = item {
                    (self.expr)(expr)?;
                }
            }
        }
        Ok(())
    }

    fn aggregation(&mut self, plan: &mut AggregationPlan) -> Result<()> {
        match &mut plan.input {
            AggregationInputPlan::Source(source) => self.source(source),
            AggregationInputPlan::InnerJoin(join) => self.inner_join(join),
            AggregationInputPlan::LeftOuterJoin(join) => self.left_outer_join(join),
            AggregationInputPlan::Filter(filter) => self.filter(filter),
        }?;
        for expr in &mut plan.group_by {
            (self.expr)(expr)?;
        }
        for aggregate in &mut plan.aggregate_slots {
            match &mut aggregate.func {
                AggregateFunctionPlan::Count(CountArgExprPlan::Wildcard) => {}
                AggregateFunctionPlan::Count(CountArgExprPlan::Expr(expr))
                | AggregateFunctionPlan::Sum(expr)
                | AggregateFunctionPlan::Min(expr)
                | AggregateFunctionPlan::Max(expr)
                | AggregateFunctionPlan::Avg(expr)
                | AggregateFunctionPlan::Variance(expr)
                | AggregateFunctionPlan::Stdev(expr) => (self.expr)(expr)?,
            }
        }
        Ok(())
    }

    fn filter(&mut self, plan: &mut FilterPlan) -> Result<()> {
        match &mut plan.input {
            FilterInputPlan::Source(source) => self.source(source),
            FilterInputPlan::InnerJoin(join) => self.inner_join(join),
            FilterInputPlan::LeftOuterJoin(join) => self.left_outer_join(join),
        }?;
        (self.expr)(&mut plan.expr)
    }

    fn having(&mut self, plan: &mut HavingPlan) -> Result<()> {
        self.aggregation(&mut plan.input)?;
        (self.expr)(&mut plan.expr)
    }

    fn inner_join(&mut self, plan: &mut InnerJoinPlan) -> Result<()> {
        match &mut plan.input {
            InnerJoinInputPlan::NestedLoop(plan) => self.nested_loop_join(plan),
            InnerJoinInputPlan::Hash(plan) => self.hash_join(plan),
            InnerJoinInputPlan::Condition(plan) => self.join_condition(plan),
        }
    }

    fn left_outer_join(&mut self, plan: &mut LeftOuterJoinPlan) -> Result<()> {
        match &mut plan.input {
            LeftOuterJoinInputPlan::NestedLoop(plan) => self.nested_loop_join(plan),
            LeftOuterJoinInputPlan::Hash(plan) => self.hash_join(plan),
            LeftOuterJoinInputPlan::Condition(plan) => self.join_condition(plan),
        }
    }

    fn join_condition(&mut self, plan: &mut JoinConditionPlan) -> Result<()> {
        match &mut plan.input {
            JoinConditionInputPlan::NestedLoop(plan) => self.nested_loop_join(plan),
            JoinConditionInputPlan::Hash(plan) => self.hash_join(plan),
        }?;
        (self.expr)(&mut plan.expr)
    }

    fn nested_loop_join(&mut self, plan: &mut NestedLoopJoinPlan) -> Result<()> {
        match &mut plan.input {
            NestedLoopJoinInputPlan::Source(source) => self.source(source),
            NestedLoopJoinInputPlan::InnerJoin(join) => self.inner_join(join),
            NestedLoopJoinInputPlan::LeftOuterJoin(join) => self.left_outer_join(join),
        }?;
        self.source(&mut plan.right)
    }

    fn hash_join(&mut self, plan: &mut HashJoinPlan) -> Result<()> {
        match &mut plan.input {
            HashJoinInputPlan::Source(source) => self.source(source),
            HashJoinInputPlan::InnerJoin(join) => self.inner_join(join),
            HashJoinInputPlan::LeftOuterJoin(join) => self.left_outer_join(join),
        }?;
        self.source(&mut plan.right)?;
        (self.expr)(&mut plan.input_key)?;
        (self.expr)(&mut plan.right_key)?;
        if let Some(expr) = &mut plan.right_filter {
            (self.expr)(expr)?;
        }
        Ok(())
    }

    fn source(&mut self, source: &mut SourcePlan) -> Result<()> {
        (self.source)(source)?;
        match source {
            SourcePlan::Table(table) => match &mut table.access {
                TableAccessPlan::PrimaryKey { expr } => (self.expr)(expr)?,
                TableAccessPlan::Index {
                    predicate: Some(predicate),
                    ..
                } => {
                    (self.expr)(&mut predicate.expr)?;
                }
                TableAccessPlan::FullScan
                | TableAccessPlan::Index {
                    predicate: None, ..
                } => {}
            },
            SourcePlan::Derived(derived) => self.query(&mut derived.query)?,
            SourcePlan::Series(series) => (self.expr)(&mut series.size)?,
            SourcePlan::Dictionary(_) => {}
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use {
        super::{QueryVisitor, try_visit_mut_query},
        crate::{
            ast::IndexOperator,
            parse_sql::{parse, parse_expr},
            plan::{
                ExprPlan, HashJoinInputPlan, HashJoinPlan, IndexPredicatePlan, InnerJoinInputPlan,
                InnerJoinPlan, JoinConditionInputPlan, JoinConditionPlan, LeftOuterJoinInputPlan,
                LeftOuterJoinPlan, ProjectInputPlan, ProjectPlan, ProjectionPlan, QueryPlan,
                SourcePlan, StatementPlanKind, TableAccessPlan, TableSourcePlan,
            },
            planner::{aggregate, expr::try_visit_mut_expr},
            result::Error,
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

    fn test(input: &str, expected: &str) {
        let mut actual = query(input);
        let result = try_visit_mut_query(
            &mut actual,
            &mut |expr| {
                try_visit_mut_expr(expr, &mut |expr| {
                    if let ExprPlan::Identifier(name) = expr {
                        *name = format!("_{name}");
                    }
                    Ok(())
                })
            },
            &mut |_| Ok(()),
        );
        assert_eq!(result, Ok(()), "{input}");
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
            let mut visited = Vec::new();
            let result = QueryVisitor {
                expr: &mut |expr: &mut ExprPlan| {
                    visited.push(expr.clone());
                    Ok(())
                },
                source: &mut |_: &SourcePlan| Ok(()),
            }
            .hash_join(&mut plan);
            assert_eq!(result, Ok(()));
            assert_eq!(visited, expected);

            for end in 0..expected.len() {
                let mut visited = Vec::new();
                let result = QueryVisitor {
                    expr: &mut |expr: &mut ExprPlan| {
                        visited.push(expr.clone());
                        if visited.len() == end + 1 {
                            Err(Error::StorageMsg("stop".to_owned()))
                        } else {
                            Ok(())
                        }
                    },
                    source: &mut |_: &SourcePlan| Ok(()),
                }
                .hash_join(&mut plan);
                assert_eq!(result, Err(Error::StorageMsg("stop".to_owned())));
                assert_eq!(visited, expected[..=end]);
            }
        }

        for (stop, expected) in [("Item", vec!["Item"]), ("Other", vec!["Item", "Other"])] {
            let mut visited = Vec::new();
            let result = QueryVisitor {
                expr: &mut |_: &mut ExprPlan| panic!("rejected source must stop before hash keys"),
                source: &mut |source: &SourcePlan| {
                    visited.push(source.alias_name().to_owned());
                    if source.alias_name() == stop {
                        Err(Error::StorageMsg("source".to_owned()))
                    } else {
                        Ok(())
                    }
                },
            }
            .hash_join(&mut hash.clone());
            assert_eq!(result, Err(Error::StorageMsg("source".to_owned())));
            assert_eq!(visited, expected);
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
            let mut visited = Vec::new();
            let result = try_visit_mut_query(
                &mut query,
                &mut |expr| {
                    visited.push(expr.clone());
                    Ok(())
                },
                &mut |_| Ok(()),
            );
            assert_eq!(result, Ok(()));
            assert_eq!(
                visited,
                vec![expr("left_key"), expr("right_key"), expr("condition")]
            );
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
        for (access, expected) in cases {
            let mut source = SourcePlan::Table(TableSourcePlan {
                name: "Item".to_owned(),
                alias: None,
                access,
            });
            let mut visited = Vec::new();
            let mut sources = Vec::new();
            let result = QueryVisitor {
                expr: &mut |expr: &mut ExprPlan| {
                    visited.push(expr.clone());
                    Ok(())
                },
                source: &mut |source: &SourcePlan| {
                    sources.push(source.clone());
                    Ok(())
                },
            }
            .source(&mut source);
            assert_eq!(result, Ok(()));
            assert_eq!(visited, expected);
            assert_eq!(sources, vec![source.clone()]);

            if !expected.is_empty() {
                let result = QueryVisitor {
                    expr: &mut |_: &mut ExprPlan| Err(Error::StorageMsg("key".to_owned())),
                    source: &mut |_: &SourcePlan| Ok(()),
                }
                .source(&mut source);
                assert_eq!(result, Err(Error::StorageMsg("key".to_owned())));
            }
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
            let mut visited = Vec::new();
            let result = try_visit_mut_query(
                &mut query,
                &mut |expr| {
                    try_visit_mut_expr(expr, &mut |expr| {
                        if let ExprPlan::Identifier(name) = expr {
                            visited.push(name.clone());
                            return Err(Error::StorageMsg("stop".to_owned()));
                        }
                        Ok(())
                    })
                },
                &mut |_| Ok(()),
            );
            assert_eq!(result, Err(Error::StorageMsg("stop".to_owned())), "{sql}");
            assert_eq!(visited, vec!["stop".to_owned()], "{sql}");
        }

        for sql in [
            "SELECT x FROM Item",
            "SELECT x FROM SERIES(n)",
            "SELECT x FROM (VALUES (y)) v",
            "SELECT x FROM GLUE_TABLES",
        ] {
            let mut query = query(sql);
            let result = try_visit_mut_query(
                &mut query,
                &mut |_| panic!("source rejection must precede expression traversal: {sql}"),
                &mut |_| Err(Error::StorageMsg("source".to_owned())),
            );
            assert_eq!(result, Err(Error::StorageMsg("source".to_owned())), "{sql}");
        }
    }
}
