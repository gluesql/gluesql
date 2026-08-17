use {
    super::expr::visit_mut_expr,
    crate::plan::{
        AggregationInputPlan, AggregationPlan, DistinctInputPlan, DistinctPlan, ExprPlan,
        FilterInputPlan, FilterPlan, HashJoinInputPlan, HashJoinPlan, HavingPlan,
        InnerJoinInputPlan, InnerJoinPlan, JoinConditionInputPlan, JoinConditionPlan,
        LeftOuterJoinInputPlan, LeftOuterJoinPlan, LimitInputPlan, LimitPlan,
        NestedLoopJoinInputPlan, NestedLoopJoinPlan, NullExtendPlan, OffsetInputPlan, OffsetPlan,
        ProjectInputPlan, ProjectPlan, ProjectionPlan, QueryPlan, RightOuterJoinInputPlan,
        RightOuterJoinPlan, SelectItemPlan, SelectOrderByPlan, SourcePlan, StatementPlan,
        TableAccessPlan, UnplannedRightOuterJoinInputPlan, UnplannedRightOuterJoinPlan,
        ValuesOrderByPlan, ValuesPlan,
    },
    std::mem,
};

/// Lowers every [`UnplannedRightOuterJoinPlan`] into a [`RightOuterJoinPlan`], deciding which
/// relations an unmatched right row NULL-extends and marking the left base source
/// [`TableAccessPlan::FullScanRequired`]. Later passes already gate their rewrites on
/// [`TableAccessPlan::FullScan`], so the marker disables them without any of them having to
/// re-interpret RIGHT JOIN syntax.
///
/// Missing a nested query here is not a lost optimization but an execution error, so this walks
/// derived sources and expression subqueries too.
///
/// Three expression positions are deliberately *not* walked, because a RIGHT JOIN cannot reach them
/// at this point in the pipeline — matching what [`super::plan_schemaless`] skips:
///
/// - `AggregationPlan::aggregate_slots` is always empty; [`super::plan_aggregate`] fills it last.
/// - `TableAccessPlan::PrimaryKey` / `Index` expressions cannot exist yet; the primary key and index
///   planners run later, and `query_builder` (the only other producer) cannot build a RIGHT JOIN.
///
/// Should any of those change, the result is an `UnreachableUnplannedRightOuterJoin` error rather
/// than a wrong answer.
pub fn plan(statement: StatementPlan) -> StatementPlan {
    match statement {
        StatementPlan::Query(query) => StatementPlan::Query(plan_query(query)),
        StatementPlan::Insert {
            table_name,
            columns,
            source,
        } => StatementPlan::Insert {
            table_name,
            columns,
            source: plan_query(source),
        },
        StatementPlan::CreateTable {
            if_not_exists,
            name,
            columns,
            source,
            engine,
            foreign_keys,
            comment,
        } => StatementPlan::CreateTable {
            if_not_exists,
            name,
            columns,
            source: source.map(|source| Box::new(plan_query(*source))),
            engine,
            foreign_keys,
            comment,
        },
        StatementPlan::Update {
            table_name,
            assignments,
            selection,
        } => StatementPlan::Update {
            table_name,
            assignments: assignments
                .into_iter()
                .map(|mut assignment| {
                    plan_expr(&mut assignment.value);
                    assignment
                })
                .collect(),
            selection: selection.map(plan_owned_expr),
        },
        StatementPlan::Delete {
            table_name,
            selection,
        } => StatementPlan::Delete {
            table_name,
            selection: selection.map(plan_owned_expr),
        },
        _ => statement,
    }
}

fn plan_query(query: QueryPlan) -> QueryPlan {
    match query {
        QueryPlan::Project(project) => QueryPlan::Project(plan_project(project)),
        QueryPlan::Values(values) => QueryPlan::Values(plan_values(values)),
        QueryPlan::SelectOrderBy(order_by) => {
            QueryPlan::SelectOrderBy(plan_select_order_by(order_by))
        }
        QueryPlan::ValuesOrderBy(order_by) => {
            QueryPlan::ValuesOrderBy(plan_values_order_by(order_by))
        }
        QueryPlan::Distinct(distinct) => QueryPlan::Distinct(plan_distinct(distinct)),
        QueryPlan::Offset(offset) => QueryPlan::Offset(plan_offset(offset)),
        QueryPlan::Limit(LimitPlan { input, count }) => {
            let input = match input {
                LimitInputPlan::Project(project) => LimitInputPlan::Project(plan_project(project)),
                LimitInputPlan::Values(values) => LimitInputPlan::Values(plan_values(values)),
                LimitInputPlan::SelectOrderBy(order_by) => {
                    LimitInputPlan::SelectOrderBy(plan_select_order_by(order_by))
                }
                LimitInputPlan::ValuesOrderBy(order_by) => {
                    LimitInputPlan::ValuesOrderBy(plan_values_order_by(order_by))
                }
                LimitInputPlan::Distinct(distinct) => {
                    LimitInputPlan::Distinct(plan_distinct(distinct))
                }
                LimitInputPlan::Offset(offset) => LimitInputPlan::Offset(plan_offset(offset)),
            };

            QueryPlan::Limit(LimitPlan {
                input,
                count: plan_owned_expr(count),
            })
        }
    }
}

fn plan_offset(OffsetPlan { input, count }: OffsetPlan) -> OffsetPlan {
    let input = match input {
        OffsetInputPlan::Project(project) => OffsetInputPlan::Project(plan_project(project)),
        OffsetInputPlan::Values(values) => OffsetInputPlan::Values(plan_values(values)),
        OffsetInputPlan::SelectOrderBy(order_by) => {
            OffsetInputPlan::SelectOrderBy(plan_select_order_by(order_by))
        }
        OffsetInputPlan::ValuesOrderBy(order_by) => {
            OffsetInputPlan::ValuesOrderBy(plan_values_order_by(order_by))
        }
        OffsetInputPlan::Distinct(distinct) => OffsetInputPlan::Distinct(plan_distinct(distinct)),
    };

    OffsetPlan {
        input,
        count: plan_owned_expr(count),
    }
}

fn plan_distinct(DistinctPlan { input }: DistinctPlan) -> DistinctPlan {
    let input = match input {
        DistinctInputPlan::Project(project) => DistinctInputPlan::Project(plan_project(project)),
        DistinctInputPlan::SelectOrderBy(order_by) => {
            DistinctInputPlan::SelectOrderBy(plan_select_order_by(order_by))
        }
    };

    DistinctPlan { input }
}

fn plan_select_order_by(
    SelectOrderByPlan { input, exprs }: SelectOrderByPlan,
) -> SelectOrderByPlan {
    SelectOrderByPlan {
        input: plan_project(input),
        exprs: exprs
            .into_iter()
            .map(|mut order_by| {
                plan_expr(&mut order_by.expr);
                order_by
            })
            .collect(),
    }
}

fn plan_values_order_by(
    ValuesOrderByPlan { input, exprs }: ValuesOrderByPlan,
) -> ValuesOrderByPlan {
    ValuesOrderByPlan {
        input: plan_values(input),
        exprs: exprs
            .into_iter()
            .map(|mut order_by| {
                plan_expr(&mut order_by.expr);
                order_by
            })
            .collect(),
    }
}

fn plan_values(ValuesPlan(rows): ValuesPlan) -> ValuesPlan {
    ValuesPlan(
        rows.into_iter()
            .map(|row| row.into_iter().map(plan_owned_expr).collect())
            .collect(),
    )
}

fn plan_project(ProjectPlan { input, projection }: ProjectPlan) -> ProjectPlan {
    let projection = match projection {
        // A select item can hold a subquery — `SELECT (SELECT … RIGHT JOIN …)` — so the projection
        // needs the same walk as the input.
        ProjectionPlan::SelectItems(items) => ProjectionPlan::SelectItems(
            items
                .into_iter()
                .map(|item| match item {
                    SelectItemPlan::Expr { expr, label } => SelectItemPlan::Expr {
                        expr: plan_owned_expr(expr),
                        label,
                    },
                    SelectItemPlan::Wildcard | SelectItemPlan::QualifiedWildcard(_) => item,
                })
                .collect(),
        ),
        ProjectionPlan::SchemalessMap => projection,
    };
    let input = match input {
        ProjectInputPlan::Source(source) => ProjectInputPlan::Source(plan_source(source)),
        ProjectInputPlan::InnerJoin(join) => {
            ProjectInputPlan::InnerJoin(Box::new(plan_inner_join(*join)))
        }
        ProjectInputPlan::LeftOuterJoin(join) => {
            ProjectInputPlan::LeftOuterJoin(Box::new(plan_left_outer_join(*join)))
        }
        ProjectInputPlan::UnplannedRightOuterJoin(join) => {
            ProjectInputPlan::RightOuterJoin(Box::new(lower(*join)))
        }
        ProjectInputPlan::RightOuterJoin(join) => {
            ProjectInputPlan::RightOuterJoin(Box::new(plan_right_outer_join(*join)))
        }
        ProjectInputPlan::Filter(filter) => ProjectInputPlan::Filter(plan_filter(filter)),
        ProjectInputPlan::Aggregation(aggregation) => {
            ProjectInputPlan::Aggregation(plan_aggregation(aggregation))
        }
        ProjectInputPlan::Having(HavingPlan { input, expr }) => {
            ProjectInputPlan::Having(HavingPlan {
                input: plan_aggregation(input),
                expr: plan_owned_expr(expr),
            })
        }
    };

    ProjectPlan { input, projection }
}

fn plan_aggregation(
    AggregationPlan {
        input,
        group_by,
        aggregate_slots,
    }: AggregationPlan,
) -> AggregationPlan {
    let input = match input {
        AggregationInputPlan::Source(source) => AggregationInputPlan::Source(plan_source(source)),
        AggregationInputPlan::InnerJoin(join) => {
            AggregationInputPlan::InnerJoin(Box::new(plan_inner_join(*join)))
        }
        AggregationInputPlan::LeftOuterJoin(join) => {
            AggregationInputPlan::LeftOuterJoin(Box::new(plan_left_outer_join(*join)))
        }
        AggregationInputPlan::UnplannedRightOuterJoin(join) => {
            AggregationInputPlan::RightOuterJoin(Box::new(lower(*join)))
        }
        AggregationInputPlan::RightOuterJoin(join) => {
            AggregationInputPlan::RightOuterJoin(Box::new(plan_right_outer_join(*join)))
        }
        AggregationInputPlan::Filter(filter) => AggregationInputPlan::Filter(plan_filter(filter)),
    };

    AggregationPlan {
        input,
        group_by: group_by.into_iter().map(plan_owned_expr).collect(),
        aggregate_slots,
    }
}

fn plan_filter(FilterPlan { input, expr }: FilterPlan) -> FilterPlan {
    let input = match input {
        FilterInputPlan::Source(source) => FilterInputPlan::Source(plan_source(source)),
        FilterInputPlan::InnerJoin(join) => {
            FilterInputPlan::InnerJoin(Box::new(plan_inner_join(*join)))
        }
        FilterInputPlan::LeftOuterJoin(join) => {
            FilterInputPlan::LeftOuterJoin(Box::new(plan_left_outer_join(*join)))
        }
        FilterInputPlan::UnplannedRightOuterJoin(join) => {
            FilterInputPlan::RightOuterJoin(Box::new(lower(*join)))
        }
        FilterInputPlan::RightOuterJoin(join) => {
            FilterInputPlan::RightOuterJoin(Box::new(plan_right_outer_join(*join)))
        }
    };

    FilterPlan {
        input,
        expr: plan_owned_expr(expr),
    }
}

fn plan_inner_join(InnerJoinPlan { input }: InnerJoinPlan) -> InnerJoinPlan {
    let input = match input {
        InnerJoinInputPlan::NestedLoop(join) => {
            InnerJoinInputPlan::NestedLoop(plan_nested_loop(join))
        }
        InnerJoinInputPlan::Hash(join) => InnerJoinInputPlan::Hash(plan_hash(join)),
        InnerJoinInputPlan::Condition(condition) => {
            InnerJoinInputPlan::Condition(plan_condition(condition))
        }
    };

    InnerJoinPlan { input }
}

fn plan_left_outer_join(LeftOuterJoinPlan { input }: LeftOuterJoinPlan) -> LeftOuterJoinPlan {
    let input = match input {
        LeftOuterJoinInputPlan::NestedLoop(join) => {
            LeftOuterJoinInputPlan::NestedLoop(plan_nested_loop(join))
        }
        LeftOuterJoinInputPlan::Hash(join) => LeftOuterJoinInputPlan::Hash(plan_hash(join)),
        LeftOuterJoinInputPlan::Condition(condition) => {
            LeftOuterJoinInputPlan::Condition(plan_condition(condition))
        }
    };

    LeftOuterJoinPlan { input }
}

fn lower(UnplannedRightOuterJoinPlan { input }: UnplannedRightOuterJoinPlan) -> RightOuterJoinPlan {
    // Lowering the mechanism first means the alias walk below sees only lowered nodes.
    let input = match input {
        UnplannedRightOuterJoinInputPlan::NestedLoop(join) => {
            RightOuterJoinInputPlan::NestedLoop(plan_nested_loop(join))
        }
        UnplannedRightOuterJoinInputPlan::Condition(condition) => {
            RightOuterJoinInputPlan::Condition(plan_condition(condition))
        }
    };
    let null_extend = NullExtendPlan {
        relations: input
            .left_sources()
            .into_iter()
            .map(|source| source.alias_name().to_owned())
            .collect(),
    };
    let mut plan = RightOuterJoinPlan { input, null_extend };

    // Idempotent: chained RIGHT JOINs all resolve to the same leftmost base source. A non-table base
    // source needs no marker, since only tables carry a narrowable access path.
    if let SourcePlan::Table(table) = plan.base_source_mut()
        && table.access == TableAccessPlan::FullScan
    {
        table.access = TableAccessPlan::FullScanRequired;
    }

    plan
}

/// An already lowered node still needs walking: its subtree can hold further RIGHT JOINs, and a
/// second run over a planned statement must be a no-op.
fn plan_right_outer_join(
    RightOuterJoinPlan { input, null_extend }: RightOuterJoinPlan,
) -> RightOuterJoinPlan {
    let input = match input {
        RightOuterJoinInputPlan::NestedLoop(join) => {
            RightOuterJoinInputPlan::NestedLoop(plan_nested_loop(join))
        }
        RightOuterJoinInputPlan::Hash(join) => RightOuterJoinInputPlan::Hash(plan_hash(join)),
        RightOuterJoinInputPlan::Condition(condition) => {
            RightOuterJoinInputPlan::Condition(plan_condition(condition))
        }
    };

    RightOuterJoinPlan { input, null_extend }
}

fn plan_condition(JoinConditionPlan { input, expr }: JoinConditionPlan) -> JoinConditionPlan {
    let input = match input {
        JoinConditionInputPlan::NestedLoop(join) => {
            JoinConditionInputPlan::NestedLoop(plan_nested_loop(join))
        }
        JoinConditionInputPlan::Hash(join) => JoinConditionInputPlan::Hash(plan_hash(join)),
    };

    JoinConditionPlan {
        input,
        expr: plan_owned_expr(expr),
    }
}

fn plan_nested_loop(NestedLoopJoinPlan { input, right }: NestedLoopJoinPlan) -> NestedLoopJoinPlan {
    let input = match input {
        NestedLoopJoinInputPlan::Source(source) => {
            NestedLoopJoinInputPlan::Source(plan_source(source))
        }
        NestedLoopJoinInputPlan::InnerJoin(join) => {
            NestedLoopJoinInputPlan::InnerJoin(Box::new(plan_inner_join(*join)))
        }
        NestedLoopJoinInputPlan::LeftOuterJoin(join) => {
            NestedLoopJoinInputPlan::LeftOuterJoin(Box::new(plan_left_outer_join(*join)))
        }
        NestedLoopJoinInputPlan::UnplannedRightOuterJoin(join) => {
            NestedLoopJoinInputPlan::RightOuterJoin(Box::new(lower(*join)))
        }
        NestedLoopJoinInputPlan::RightOuterJoin(join) => {
            NestedLoopJoinInputPlan::RightOuterJoin(Box::new(plan_right_outer_join(*join)))
        }
    };

    NestedLoopJoinPlan {
        input,
        right: plan_source(right),
    }
}

fn plan_hash(
    HashJoinPlan {
        input,
        right,
        input_key,
        right_key,
        right_filter,
    }: HashJoinPlan,
) -> HashJoinPlan {
    let input = match input {
        HashJoinInputPlan::Source(source) => HashJoinInputPlan::Source(plan_source(source)),
        HashJoinInputPlan::InnerJoin(join) => {
            HashJoinInputPlan::InnerJoin(Box::new(plan_inner_join(*join)))
        }
        HashJoinInputPlan::LeftOuterJoin(join) => {
            HashJoinInputPlan::LeftOuterJoin(Box::new(plan_left_outer_join(*join)))
        }
        HashJoinInputPlan::RightOuterJoin(join) => {
            HashJoinInputPlan::RightOuterJoin(Box::new(plan_right_outer_join(*join)))
        }
    };

    HashJoinPlan {
        input,
        right: plan_source(right),
        input_key: plan_owned_expr(input_key),
        right_key: plan_owned_expr(right_key),
        right_filter: right_filter.map(plan_owned_expr),
    }
}

fn plan_source(source: SourcePlan) -> SourcePlan {
    match source {
        SourcePlan::Derived(mut derived) => {
            *derived.query = plan_query(mem::replace(
                derived.query.as_mut(),
                QueryPlan::Values(ValuesPlan(Vec::new())),
            ));

            SourcePlan::Derived(derived)
        }
        SourcePlan::Series(mut series) => {
            plan_expr(&mut series.size);

            SourcePlan::Series(series)
        }
        SourcePlan::Table(_) | SourcePlan::Dictionary(_) => source,
    }
}

fn plan_owned_expr(mut expr: ExprPlan) -> ExprPlan {
    plan_expr(&mut expr);

    expr
}

fn plan_expr(expr: &mut ExprPlan) {
    visit_mut_expr(expr, &mut |expr| match expr {
        ExprPlan::Subquery(query)
        | ExprPlan::Exists {
            subquery: query, ..
        }
        | ExprPlan::InSubquery {
            subquery: query, ..
        } => {
            // `plan_query` needs ownership, so the subquery is swapped out for an empty
            // `QueryPlan` and swapped back once it has been planned.
            **query = plan_query(mem::replace(
                query.as_mut(),
                QueryPlan::Values(ValuesPlan(Vec::new())),
            ));
        }
        _ => {}
    });
}

#[cfg(test)]
mod tests {
    use {
        super::plan,
        crate::{
            parse_sql::parse,
            plan::{
                AggregationInputPlan, ExprPlan, FilterInputPlan, InnerJoinInputPlan,
                JoinConditionInputPlan, NestedLoopJoinInputPlan, NullExtendPlan, ProjectInputPlan,
                QueryPlan, RightOuterJoinInputPlan, RightOuterJoinPlan, SourcePlan, StatementPlan,
                TableAccessPlan,
            },
            translate::translate,
        },
        pretty_assertions::assert_eq,
    };

    fn statement(sql: &str) -> StatementPlan {
        let parsed = parse(sql).expect(sql).into_iter().next().expect(sql);

        StatementPlan::from(translate(&parsed).expect(sql))
    }

    fn plan_sql(sql: &str) -> StatementPlan {
        plan(statement(sql))
    }

    fn project_input(query: &QueryPlan) -> &ProjectInputPlan {
        &query.project().expect("expected a project").input
    }

    fn query_input(statement: &StatementPlan) -> &ProjectInputPlan {
        let StatementPlan::Query(query) = statement else {
            panic!("expected a query");
        };

        project_input(query)
    }

    fn right_outer(input: &ProjectInputPlan) -> &RightOuterJoinPlan {
        let ProjectInputPlan::RightOuterJoin(join) = input else {
            panic!("expected a lowered right outer join");
        };

        join
    }

    fn relations(join: &RightOuterJoinPlan) -> &[String] {
        &join.null_extend.relations
    }

    fn collect_relations(statement: &StatementPlan, found: &mut Vec<Vec<String>>) {
        let json = serde_json::to_value(statement).expect("serializable");
        walk_json(&json, found);
    }

    fn walk_json(value: &serde_json::Value, found: &mut Vec<Vec<String>>) {
        match value {
            serde_json::Value::Object(map) => {
                assert!(
                    !map.contains_key("UnplannedRightOuterJoin"),
                    "an unplanned right outer join survived planning"
                );
                if let Some(null_extend) = map.get("null_extend")
                    && let Some(relations) = null_extend.get("relations")
                {
                    found.push(
                        relations
                            .as_array()
                            .expect("relations is an array")
                            .iter()
                            .map(|relation| relation.as_str().expect("alias").to_owned())
                            .collect(),
                    );
                }
                for nested in map.values() {
                    walk_json(nested, found);
                }
            }
            serde_json::Value::Array(items) => {
                for nested in items {
                    walk_json(nested, found);
                }
            }
            _ => {}
        }
    }

    fn subquery(expr: &ExprPlan) -> &QueryPlan {
        match expr {
            ExprPlan::Subquery(query)
            | ExprPlan::Exists {
                subquery: query, ..
            }
            | ExprPlan::InSubquery {
                subquery: query, ..
            } => query,
            _ => panic!("expected a subquery"),
        }
    }

    #[test]
    fn lowers_right_outer_join_and_collects_null_extend_relations() {
        let statement = plan_sql("SELECT * FROM A RIGHT JOIN B ON A.id = B.a_id");
        let join = right_outer(query_input(&statement));

        assert!(matches!(join.input, RightOuterJoinInputPlan::Condition(_)));
        assert_eq!(
            join.null_extend,
            NullExtendPlan {
                relations: vec!["A".to_owned()],
            }
        );

        let statement = plan_sql("SELECT * FROM A RIGHT JOIN B");
        let join = right_outer(query_input(&statement));

        assert!(matches!(join.input, RightOuterJoinInputPlan::NestedLoop(_)));
        assert_eq!(relations(join), ["A".to_owned()]);
    }

    #[test]
    fn null_extend_accumulates_the_whole_left_prefix() {
        let statement = plan_sql("SELECT * FROM A JOIN B ON A.id = B.a_id RIGHT JOIN C");
        let join = right_outer(query_input(&statement));

        assert_eq!(relations(join), ["A".to_owned(), "B".to_owned()]);

        let statement = plan_sql("SELECT * FROM A a1 RIGHT JOIN B b1 RIGHT JOIN C c1");
        let outer = right_outer(query_input(&statement));

        assert_eq!(relations(outer), ["a1".to_owned(), "b1".to_owned()]);

        let RightOuterJoinInputPlan::NestedLoop(nested_loop) = &outer.input else {
            panic!("expected a nested loop mechanism");
        };
        let NestedLoopJoinInputPlan::RightOuterJoin(inner) = &nested_loop.input else {
            panic!("expected the inner join to be lowered too");
        };

        assert_eq!(relations(inner), ["a1".to_owned()]);
    }

    #[test]
    fn pins_the_left_base_source_to_a_full_scan() {
        let statement = plan_sql("SELECT * FROM A RIGHT JOIN B RIGHT JOIN C WHERE A.id = 1");
        let ProjectInputPlan::Filter(filter) = query_input(&statement) else {
            panic!("expected a filter");
        };
        let FilterInputPlan::RightOuterJoin(join) = &filter.input else {
            panic!("expected a lowered right outer join");
        };
        let SourcePlan::Table(table) = join.base_source() else {
            panic!("expected a table base source");
        };

        assert_eq!(table.name, "A");
        assert_eq!(table.access, TableAccessPlan::FullScanRequired);
    }

    #[test]
    fn leaves_a_non_table_left_base_source_alone() {
        let statement = plan_sql("SELECT * FROM (SELECT * FROM A) d RIGHT JOIN B");
        let join = right_outer(query_input(&statement));

        assert!(matches!(join.base_source(), SourcePlan::Derived(_)));
        assert_eq!(relations(join), ["d".to_owned()]);
    }

    #[test]
    fn lowers_right_outer_joins_inside_derived_sources_and_subqueries() {
        let statement =
            plan_sql("SELECT * FROM (SELECT B.id FROM A RIGHT JOIN B ON A.id = B.a_id) d");
        let SourcePlan::Derived(derived) = query_input(&statement).base_source() else {
            panic!("expected a derived source");
        };
        let join = right_outer(project_input(&derived.query));

        assert_eq!(relations(join), ["A".to_owned()]);

        let statement = plan_sql("SELECT * FROM C WHERE C.id IN (SELECT B.id FROM A RIGHT JOIN B)");
        let ProjectInputPlan::Filter(filter) = query_input(&statement) else {
            panic!("expected a filter");
        };
        let join = right_outer(project_input(subquery(&filter.expr)));

        assert_eq!(relations(join), ["A".to_owned()]);
    }

    #[test]
    fn lowers_right_outer_joins_in_every_statement_position() {
        for sql in [
            "INSERT INTO C SELECT B.id FROM A RIGHT JOIN B",
            "CREATE TABLE D AS SELECT B.id FROM A RIGHT JOIN B",
        ] {
            let planned = plan_sql(sql);
            let source = match &planned {
                StatementPlan::Insert { source, .. } => source,
                StatementPlan::CreateTable { source, .. } => {
                    source.as_deref().expect("expected a source")
                }
                _ => panic!("unexpected statement: {sql}"),
            };
            let join = right_outer(project_input(source));

            assert_eq!(relations(join), ["A".to_owned()], "{sql}");
        }

        let planned = plan_sql(
            "UPDATE C SET id = (SELECT B.id FROM A RIGHT JOIN B) WHERE EXISTS (SELECT * FROM A a2 RIGHT JOIN B)",
        );
        let StatementPlan::Update {
            assignments,
            selection,
            ..
        } = &planned
        else {
            panic!("expected an update");
        };
        let join = right_outer(project_input(subquery(&assignments[0].value)));
        assert_eq!(relations(join), ["A".to_owned()]);

        let selection = selection.as_ref().expect("expected a selection");
        let join = right_outer(project_input(subquery(selection)));
        assert_eq!(relations(join), ["a2".to_owned()]);

        let planned = plan_sql("DELETE FROM C WHERE EXISTS (SELECT * FROM A RIGHT JOIN B)");
        let StatementPlan::Delete { selection, .. } = &planned else {
            panic!("expected a delete");
        };
        let selection = selection.as_ref().expect("expected a selection");
        let join = right_outer(project_input(subquery(selection)));

        assert_eq!(relations(join), ["A".to_owned()]);
    }

    #[test]
    fn lowers_a_right_outer_join_inside_a_select_item() {
        // A select-item subquery is the one expression position that is not reachable from
        // `ProjectInputPlan`, so it needs its own walk.
        for sql in [
            "SELECT (SELECT B.id FROM A RIGHT JOIN B) AS s FROM C",
            "SELECT SUM((SELECT B.id FROM A RIGHT JOIN B)) AS s FROM C",
            "SELECT C.id FROM C WHERE EXISTS (SELECT (SELECT B.id FROM A RIGHT JOIN B) FROM C)",
        ] {
            let planned = plan_sql(sql);
            let mut found = Vec::new();
            collect_relations(&planned, &mut found);

            assert_eq!(found, vec![vec!["A".to_owned()]], "{sql}");
        }
    }

    #[test]
    fn lowers_a_right_outer_join_in_a_values_expression() {
        let planned = plan_sql("VALUES ((SELECT B.id FROM A RIGHT JOIN B))");
        let StatementPlan::Query(QueryPlan::Values(values)) = &planned else {
            panic!("expected values");
        };
        let join = right_outer(project_input(subquery(&values.0[0][0])));

        assert_eq!(relations(join), ["A".to_owned()]);
    }

    #[test]
    fn no_unplanned_right_outer_join_survives_any_expression_position() {
        let right_join = "SELECT B.id FROM A RIGHT JOIN B";

        for sql in [
            format!("SELECT ({right_join}) AS s FROM C"),
            format!("SELECT SUM(({right_join})) AS s FROM C"),
            format!("SELECT ABS(({right_join})) AS s FROM C"),
            format!("SELECT * FROM C WHERE C.id IN ({right_join})"),
            format!("SELECT * FROM C JOIN D ON D.id = ({right_join})"),
            format!("SELECT * FROM C LEFT JOIN D ON EXISTS ({right_join})"),
            format!("SELECT * FROM ({right_join}) d"),
            format!("SELECT * FROM C ORDER BY ({right_join})"),
            format!("SELECT * FROM C LIMIT ({right_join})"),
            format!("SELECT * FROM C OFFSET ({right_join})"),
            format!("SELECT C.id FROM C GROUP BY C.id HAVING EXISTS ({right_join})"),
            format!("SELECT C.id FROM C GROUP BY ({right_join})"),
            format!("SELECT * FROM SERIES(({right_join})) s"),
            format!("VALUES (({right_join}))"),
            format!("INSERT INTO C VALUES (({right_join}))"),
            format!("CREATE TABLE E AS SELECT ({right_join}) AS s FROM C"),
            format!("UPDATE C SET id = ({right_join})"),
            format!("UPDATE C SET id = 1 WHERE id = ({right_join})"),
            format!("DELETE FROM C WHERE id = ({right_join})"),
        ] {
            let planned = plan_sql(&sql);
            let mut found = Vec::new();
            collect_relations(&planned, &mut found);

            assert_eq!(found, vec![vec!["A".to_owned()]], "{sql}");
        }
    }

    #[test]
    fn planning_an_already_planned_statement_changes_nothing() {
        let planned = plan_sql("SELECT * FROM A RIGHT JOIN B ON A.id = B.a_id WHERE A.id = 1");

        assert_eq!(plan(planned.clone()), planned);
    }

    #[test]
    fn leaves_statements_without_a_right_outer_join_alone() {
        for sql in [
            "SELECT * FROM A JOIN B ON A.id = B.a_id LEFT JOIN C",
            "SELECT * FROM A WHERE id IN (SELECT id FROM B) GROUP BY id HAVING id > 0",
            "SELECT * FROM SERIES(3) s ORDER BY N LIMIT 1 OFFSET 1",
            "SELECT * FROM A ORDER BY id OFFSET 1",
            "VALUES (1), (2)",
            "VALUES (1) ORDER BY column1 LIMIT 1 OFFSET 1",
            "SELECT DISTINCT * FROM GLUE_TABLES g",
            "UPDATE A SET id = 1 WHERE id = 2",
            "DELETE FROM A WHERE id = 1",
            "CREATE TABLE B AS SELECT * FROM A",
            "DROP TABLE A",
        ] {
            let statement = statement(sql);

            assert_eq!(plan(statement.clone()), statement, "{sql}");
        }
    }

    #[test]
    fn lowers_a_right_outer_join_under_an_aggregation() {
        let statement = plan_sql(
            "SELECT B.id FROM A RIGHT JOIN B ON A.id = B.a_id GROUP BY B.id HAVING B.id > 0",
        );
        let ProjectInputPlan::Having(having) = query_input(&statement) else {
            panic!("expected a having");
        };
        let AggregationInputPlan::RightOuterJoin(join) = &having.input.input else {
            panic!("expected a lowered right outer join");
        };

        assert_eq!(relations(join), ["A".to_owned()]);

        let statement = plan_sql("SELECT B.id FROM A RIGHT JOIN B GROUP BY B.id");
        let ProjectInputPlan::Aggregation(aggregation) = query_input(&statement) else {
            panic!("expected an aggregation");
        };
        let AggregationInputPlan::RightOuterJoin(join) = &aggregation.input else {
            panic!("expected a lowered right outer join");
        };

        assert_eq!(relations(join), ["A".to_owned()]);
    }

    #[test]
    fn lowers_a_right_outer_join_feeding_a_later_query_stage() {
        for sql in [
            "SELECT * FROM A RIGHT JOIN B ORDER BY B.id",
            "SELECT DISTINCT * FROM A RIGHT JOIN B",
            "SELECT * FROM A RIGHT JOIN B OFFSET 1",
            "SELECT * FROM A RIGHT JOIN B LIMIT 1",
            "SELECT DISTINCT * FROM A RIGHT JOIN B ORDER BY B.id LIMIT 1 OFFSET 1",
            "SELECT DISTINCT * FROM A RIGHT JOIN B ORDER BY B.id OFFSET 1",
        ] {
            let statement = plan_sql(sql);
            let join = right_outer(query_input(&statement));

            assert_eq!(relations(join), ["A".to_owned()], "{sql}");
        }
    }

    #[test]
    fn lowers_a_right_outer_join_feeding_a_later_join() {
        let statement = plan_sql("SELECT * FROM A RIGHT JOIN B JOIN C ON C.id = B.id");
        let ProjectInputPlan::InnerJoin(inner) = query_input(&statement) else {
            panic!("expected an inner join");
        };
        let InnerJoinInputPlan::Condition(condition) = &inner.input else {
            panic!("expected a join condition");
        };
        let JoinConditionInputPlan::NestedLoop(nested_loop) = &condition.input else {
            panic!("expected a nested loop mechanism");
        };
        let NestedLoopJoinInputPlan::RightOuterJoin(join) = &nested_loop.input else {
            panic!("expected a lowered right outer join");
        };

        assert_eq!(relations(join), ["A".to_owned()]);
    }

    #[test]
    fn query_plan_keeps_the_right_outer_join_unplanned_before_this_pass() {
        assert!(matches!(
            query_input(&statement("SELECT * FROM A RIGHT JOIN B")),
            ProjectInputPlan::UnplannedRightOuterJoin(_)
        ));
    }
}
