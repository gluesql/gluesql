mod aggregation;
mod distinct;
mod filter;
mod having;
mod limit;
mod offset;
mod order_by_expr;
mod project;
mod select_order_by;
mod values;

pub use {
    aggregation::{AggregationInputPlan, AggregationPlan},
    distinct::{DistinctInputPlan, DistinctPlan},
    filter::{FilterInputPlan, FilterPlan},
    having::HavingPlan,
    limit::{LimitInputPlan, LimitPlan},
    offset::{OffsetInputPlan, OffsetPlan},
    order_by_expr::OrderByExprPlan,
    project::{ProjectInputPlan, ProjectPlan},
    select_order_by::SelectOrderByPlan,
    values::{ValuesOrderByPlan, ValuesPlan},
};

use {
    crate::{
        ast,
        plan::{
            InnerJoinInputPlan, InnerJoinPlan, JoinConditionInputPlan, JoinConditionPlan,
            LeftOuterJoinInputPlan, LeftOuterJoinPlan, NestedLoopJoinInputPlan, NestedLoopJoinPlan,
        },
    },
    serde::{Deserialize, Serialize},
};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum QueryPlan {
    Project(ProjectPlan),
    Values(ValuesPlan),
    SelectOrderBy(SelectOrderByPlan),
    ValuesOrderBy(ValuesOrderByPlan),
    Distinct(DistinctPlan),
    Offset(OffsetPlan),
    Limit(LimitPlan),
}

impl From<ast::Query> for QueryPlan {
    fn from(query: ast::Query) -> Self {
        let ast::Query {
            body,
            order_by,
            limit,
            offset,
        } = query;

        let order_by = order_by.into_iter().map(Into::into).collect::<Vec<_>>();

        match body {
            ast::SetExpr::Select(select) => {
                let ast::Select {
                    distinct,
                    projection,
                    from,
                    selection,
                    group_by,
                    having,
                } = *select;
                let ast::TableWithJoins { relation, joins } = from;
                let source = joins.into_iter().fold(
                    NestedLoopJoinInputPlan::Source(relation.into()),
                    |input, join| {
                        let ast::Join {
                            relation,
                            join_operator,
                        } = join;
                        let nested_loop = NestedLoopJoinPlan {
                            input,
                            right: relation.into(),
                        };

                        match join_operator {
                            ast::JoinOperator::Inner(ast::JoinConstraint::None) => {
                                NestedLoopJoinInputPlan::InnerJoin(Box::new(InnerJoinPlan {
                                    input: InnerJoinInputPlan::NestedLoop(nested_loop),
                                }))
                            }
                            ast::JoinOperator::Inner(ast::JoinConstraint::On(expr)) => {
                                NestedLoopJoinInputPlan::InnerJoin(Box::new(InnerJoinPlan {
                                    input: InnerJoinInputPlan::Condition(JoinConditionPlan {
                                        input: JoinConditionInputPlan::NestedLoop(nested_loop),
                                        expr: expr.into(),
                                    }),
                                }))
                            }
                            ast::JoinOperator::LeftOuter(ast::JoinConstraint::None) => {
                                NestedLoopJoinInputPlan::LeftOuterJoin(Box::new(
                                    LeftOuterJoinPlan {
                                        input: LeftOuterJoinInputPlan::NestedLoop(nested_loop),
                                    },
                                ))
                            }
                            ast::JoinOperator::LeftOuter(ast::JoinConstraint::On(expr)) => {
                                NestedLoopJoinInputPlan::LeftOuterJoin(Box::new(
                                    LeftOuterJoinPlan {
                                        input: LeftOuterJoinInputPlan::Condition(
                                            JoinConditionPlan {
                                                input: JoinConditionInputPlan::NestedLoop(
                                                    nested_loop,
                                                ),
                                                expr: expr.into(),
                                            },
                                        ),
                                    },
                                ))
                            }
                        }
                    },
                );
                let input = match selection {
                    Some(expr) => AggregationInputPlan::Filter(FilterPlan {
                        input: match source {
                            NestedLoopJoinInputPlan::Source(source) => {
                                FilterInputPlan::Source(source)
                            }
                            NestedLoopJoinInputPlan::InnerJoin(join) => {
                                FilterInputPlan::InnerJoin(join)
                            }
                            NestedLoopJoinInputPlan::LeftOuterJoin(join) => {
                                FilterInputPlan::LeftOuterJoin(join)
                            }
                        },
                        expr: expr.into(),
                    }),
                    None => match source {
                        NestedLoopJoinInputPlan::Source(source) => {
                            AggregationInputPlan::Source(source)
                        }
                        NestedLoopJoinInputPlan::InnerJoin(join) => {
                            AggregationInputPlan::InnerJoin(join)
                        }
                        NestedLoopJoinInputPlan::LeftOuterJoin(join) => {
                            AggregationInputPlan::LeftOuterJoin(join)
                        }
                    },
                };
                let group_by = group_by.into_iter().map(Into::into).collect::<Vec<_>>();
                let input = match having {
                    Some(having) => ProjectInputPlan::Having(HavingPlan {
                        input: AggregationPlan {
                            input,
                            group_by,
                            aggregate_slots: Vec::new(),
                        },
                        expr: having.into(),
                    }),
                    None if group_by.is_empty() => match input {
                        AggregationInputPlan::Source(source) => ProjectInputPlan::Source(source),
                        AggregationInputPlan::InnerJoin(join) => ProjectInputPlan::InnerJoin(join),
                        AggregationInputPlan::LeftOuterJoin(join) => {
                            ProjectInputPlan::LeftOuterJoin(join)
                        }
                        AggregationInputPlan::Filter(filter) => ProjectInputPlan::Filter(filter),
                    },
                    None => ProjectInputPlan::Aggregation(AggregationPlan {
                        input,
                        group_by,
                        aggregate_slots: Vec::new(),
                    }),
                };
                let input = ProjectPlan {
                    input,
                    projection: projection.into(),
                };

                match (distinct, order_by.is_empty(), offset, limit) {
                    (false, true, None, None) => Self::Project(input),
                    (false, false, None, None) => Self::SelectOrderBy(SelectOrderByPlan {
                        input,
                        exprs: order_by,
                    }),
                    (false, true, Some(offset), None) => Self::Offset(OffsetPlan {
                        input: OffsetInputPlan::Project(input),
                        count: offset.into(),
                    }),
                    (false, false, Some(offset), None) => Self::Offset(OffsetPlan {
                        input: OffsetInputPlan::SelectOrderBy(SelectOrderByPlan {
                            input,
                            exprs: order_by,
                        }),
                        count: offset.into(),
                    }),
                    (false, true, None, Some(limit)) => Self::Limit(LimitPlan {
                        input: LimitInputPlan::Project(input),
                        count: limit.into(),
                    }),
                    (false, false, None, Some(limit)) => Self::Limit(LimitPlan {
                        input: LimitInputPlan::SelectOrderBy(SelectOrderByPlan {
                            input,
                            exprs: order_by,
                        }),
                        count: limit.into(),
                    }),
                    (false, true, Some(offset), Some(limit)) => {
                        let offset = OffsetPlan {
                            input: OffsetInputPlan::Project(input),
                            count: offset.into(),
                        };

                        Self::Limit(LimitPlan {
                            input: LimitInputPlan::Offset(offset),
                            count: limit.into(),
                        })
                    }
                    (false, false, Some(offset), Some(limit)) => {
                        let order_by = SelectOrderByPlan {
                            input,
                            exprs: order_by,
                        };
                        let offset = OffsetPlan {
                            input: OffsetInputPlan::SelectOrderBy(order_by),
                            count: offset.into(),
                        };

                        Self::Limit(LimitPlan {
                            input: LimitInputPlan::Offset(offset),
                            count: limit.into(),
                        })
                    }
                    (true, true, None, None) => Self::Distinct(DistinctPlan {
                        input: DistinctInputPlan::Project(input),
                    }),
                    (true, false, None, None) => Self::Distinct(DistinctPlan {
                        input: DistinctInputPlan::SelectOrderBy(SelectOrderByPlan {
                            input,
                            exprs: order_by,
                        }),
                    }),
                    (true, true, Some(offset), None) => Self::Offset(OffsetPlan {
                        input: OffsetInputPlan::Distinct(DistinctPlan {
                            input: DistinctInputPlan::Project(input),
                        }),
                        count: offset.into(),
                    }),
                    (true, false, Some(offset), None) => Self::Offset(OffsetPlan {
                        input: OffsetInputPlan::Distinct(DistinctPlan {
                            input: DistinctInputPlan::SelectOrderBy(SelectOrderByPlan {
                                input,
                                exprs: order_by,
                            }),
                        }),
                        count: offset.into(),
                    }),
                    (true, true, None, Some(limit)) => Self::Limit(LimitPlan {
                        input: LimitInputPlan::Distinct(DistinctPlan {
                            input: DistinctInputPlan::Project(input),
                        }),
                        count: limit.into(),
                    }),
                    (true, false, None, Some(limit)) => Self::Limit(LimitPlan {
                        input: LimitInputPlan::Distinct(DistinctPlan {
                            input: DistinctInputPlan::SelectOrderBy(SelectOrderByPlan {
                                input,
                                exprs: order_by,
                            }),
                        }),
                        count: limit.into(),
                    }),
                    (true, true, Some(offset), Some(limit)) => {
                        let offset = OffsetPlan {
                            input: OffsetInputPlan::Distinct(DistinctPlan {
                                input: DistinctInputPlan::Project(input),
                            }),
                            count: offset.into(),
                        };

                        Self::Limit(LimitPlan {
                            input: LimitInputPlan::Offset(offset),
                            count: limit.into(),
                        })
                    }
                    (true, false, Some(offset), Some(limit)) => {
                        let order_by = SelectOrderByPlan {
                            input,
                            exprs: order_by,
                        };
                        let distinct = DistinctPlan {
                            input: DistinctInputPlan::SelectOrderBy(order_by),
                        };
                        let offset = OffsetPlan {
                            input: OffsetInputPlan::Distinct(distinct),
                            count: offset.into(),
                        };

                        Self::Limit(LimitPlan {
                            input: LimitInputPlan::Offset(offset),
                            count: limit.into(),
                        })
                    }
                }
            }
            ast::SetExpr::Values(values) => {
                let input = values.into();

                match (order_by.is_empty(), offset, limit) {
                    (true, None, None) => Self::Values(input),
                    (false, None, None) => Self::ValuesOrderBy(ValuesOrderByPlan {
                        input,
                        exprs: order_by,
                    }),
                    (true, Some(offset), None) => Self::Offset(OffsetPlan {
                        input: OffsetInputPlan::Values(input),
                        count: offset.into(),
                    }),
                    (false, Some(offset), None) => Self::Offset(OffsetPlan {
                        input: OffsetInputPlan::ValuesOrderBy(ValuesOrderByPlan {
                            input,
                            exprs: order_by,
                        }),
                        count: offset.into(),
                    }),
                    (true, None, Some(limit)) => Self::Limit(LimitPlan {
                        input: LimitInputPlan::Values(input),
                        count: limit.into(),
                    }),
                    (false, None, Some(limit)) => Self::Limit(LimitPlan {
                        input: LimitInputPlan::ValuesOrderBy(ValuesOrderByPlan {
                            input,
                            exprs: order_by,
                        }),
                        count: limit.into(),
                    }),
                    (true, Some(offset), Some(limit)) => {
                        let offset = OffsetPlan {
                            input: OffsetInputPlan::Values(input),
                            count: offset.into(),
                        };

                        Self::Limit(LimitPlan {
                            input: LimitInputPlan::Offset(offset),
                            count: limit.into(),
                        })
                    }
                    (false, Some(offset), Some(limit)) => {
                        let order_by = ValuesOrderByPlan {
                            input,
                            exprs: order_by,
                        };
                        let offset = OffsetPlan {
                            input: OffsetInputPlan::ValuesOrderBy(order_by),
                            count: offset.into(),
                        };

                        Self::Limit(LimitPlan {
                            input: LimitInputPlan::Offset(offset),
                            count: limit.into(),
                        })
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::{
            AggregationInputPlan, AggregationPlan, DistinctInputPlan, DistinctPlan, FilterPlan,
            HavingPlan, LimitInputPlan, LimitPlan, OffsetInputPlan, OffsetPlan, ProjectInputPlan,
            ProjectPlan, QueryPlan,
        },
        crate::{
            ast::{BinaryOperator, Literal},
            data::Value,
            parse_sql::parse,
            plan::{
                ExprPlan, FilterInputPlan, InnerJoinInputPlan, InnerJoinPlan,
                JoinConditionInputPlan, JoinConditionPlan, LeftOuterJoinInputPlan,
                LeftOuterJoinPlan, NestedLoopJoinInputPlan, NestedLoopJoinPlan, ProjectionPlan,
                SelectItemPlan, SelectOrderByPlan, SourcePlan, StatementPlan, TableAccessPlan,
                TableSourcePlan, ValuesOrderByPlan,
            },
            translate::translate,
        },
        pretty_assertions::assert_eq,
    };

    fn statement_plan(sql: &str) -> StatementPlan {
        parse(sql)
            .and_then(|mut statements| translate(&statements.remove(0)))
            .map(StatementPlan::from)
            .unwrap()
    }

    fn relation_plan() -> SourcePlan {
        SourcePlan::Table(TableSourcePlan {
            name: "Item".to_owned(),
            alias: None,
            access: TableAccessPlan::FullScan,
        })
    }

    fn filter_plan() -> FilterPlan {
        FilterPlan {
            input: FilterInputPlan::Source(relation_plan()),
            expr: ExprPlan::Identifier("active".to_owned()),
        }
    }

    fn project_statement(input: ProjectInputPlan) -> StatementPlan {
        StatementPlan::Query(QueryPlan::Project(ProjectPlan {
            input,
            projection: ProjectionPlan::SelectItems(vec![SelectItemPlan::Wildcard]),
        }))
    }

    #[test]
    fn query_plan_preserves_typed_select_aggregation_having_relations() {
        assert_eq!(
            statement_plan("SELECT * FROM Item"),
            project_statement(ProjectInputPlan::Source(relation_plan()))
        );
        assert_eq!(
            statement_plan("SELECT * FROM Item WHERE active"),
            project_statement(ProjectInputPlan::Filter(filter_plan()))
        );
        assert_eq!(
            statement_plan("SELECT * FROM Item GROUP BY category"),
            project_statement(ProjectInputPlan::Aggregation(AggregationPlan {
                input: AggregationInputPlan::Source(relation_plan()),
                group_by: vec![ExprPlan::Identifier("category".to_owned())],
                aggregate_slots: Vec::new(),
            }))
        );
        assert_eq!(
            statement_plan("SELECT * FROM Item WHERE active GROUP BY category"),
            project_statement(ProjectInputPlan::Aggregation(AggregationPlan {
                input: AggregationInputPlan::Filter(filter_plan()),
                group_by: vec![ExprPlan::Identifier("category".to_owned())],
                aggregate_slots: Vec::new(),
            }))
        );
        assert_eq!(
            statement_plan("SELECT * FROM Item GROUP BY category HAVING TRUE"),
            project_statement(ProjectInputPlan::Having(HavingPlan {
                input: AggregationPlan {
                    input: AggregationInputPlan::Source(relation_plan()),
                    group_by: vec![ExprPlan::Identifier("category".to_owned())],
                    aggregate_slots: Vec::new(),
                },
                expr: ExprPlan::Value(Value::Bool(true)),
            }))
        );
        assert_eq!(
            statement_plan("SELECT * FROM Item WHERE active GROUP BY category HAVING TRUE"),
            project_statement(ProjectInputPlan::Having(HavingPlan {
                input: AggregationPlan {
                    input: AggregationInputPlan::Filter(filter_plan()),
                    group_by: vec![ExprPlan::Identifier("category".to_owned())],
                    aggregate_slots: Vec::new(),
                },
                expr: ExprPlan::Value(Value::Bool(true)),
            }))
        );
        assert_eq!(
            statement_plan("SELECT * FROM Item HAVING TRUE"),
            project_statement(ProjectInputPlan::Having(HavingPlan {
                input: AggregationPlan {
                    input: AggregationInputPlan::Source(relation_plan()),
                    group_by: Vec::new(),
                    aggregate_slots: Vec::new(),
                },
                expr: ExprPlan::Value(Value::Bool(true)),
            }))
        );
        assert_eq!(
            statement_plan("SELECT * FROM Item WHERE active HAVING TRUE"),
            project_statement(ProjectInputPlan::Having(HavingPlan {
                input: AggregationPlan {
                    input: AggregationInputPlan::Filter(filter_plan()),
                    group_by: Vec::new(),
                    aggregate_slots: Vec::new(),
                },
                expr: ExprPlan::Value(Value::Bool(true)),
            }))
        );
    }

    #[test]
    fn query_plan_preserves_filter_through_terminal_stages() {
        let actual =
            statement_plan("SELECT DISTINCT * FROM Item WHERE active ORDER BY id LIMIT 3 OFFSET 2");
        let project = ProjectPlan {
            input: ProjectInputPlan::Filter(filter_plan()),
            projection: ProjectionPlan::SelectItems(vec![SelectItemPlan::Wildcard]),
        };
        let order_by = SelectOrderByPlan {
            input: project,
            exprs: vec![super::OrderByExprPlan {
                expr: ExprPlan::Identifier("id".to_owned()),
                asc: None,
            }],
        };
        let distinct = DistinctPlan {
            input: DistinctInputPlan::SelectOrderBy(order_by),
        };
        let offset = OffsetPlan {
            input: OffsetInputPlan::Distinct(distinct),
            count: ExprPlan::Literal(Literal::Number(2.into())),
        };
        let expected = StatementPlan::Query(QueryPlan::Limit(LimitPlan {
            input: LimitInputPlan::Offset(offset),
            count: ExprPlan::Literal(Literal::Number(3.into())),
        }));

        assert_eq!(actual, expected);
    }

    #[test]
    fn query_plan_builds_left_deep_join_pipeline() {
        let actual = statement_plan("SELECT * FROM A JOIN B ON A.id = B.a_id LEFT JOIN C");
        let first_join = InnerJoinPlan {
            input: InnerJoinInputPlan::Condition(JoinConditionPlan {
                input: JoinConditionInputPlan::NestedLoop(NestedLoopJoinPlan {
                    input: NestedLoopJoinInputPlan::Source(SourcePlan::Table(TableSourcePlan {
                        name: "A".to_owned(),
                        alias: None,
                        access: TableAccessPlan::FullScan,
                    })),
                    right: SourcePlan::Table(TableSourcePlan {
                        name: "B".to_owned(),
                        alias: None,
                        access: TableAccessPlan::FullScan,
                    }),
                }),
                expr: ExprPlan::BinaryOp {
                    left: Box::new(ExprPlan::CompoundIdentifier {
                        alias: "A".to_owned(),
                        ident: "id".to_owned(),
                    }),
                    op: BinaryOperator::Eq,
                    right: Box::new(ExprPlan::CompoundIdentifier {
                        alias: "B".to_owned(),
                        ident: "a_id".to_owned(),
                    }),
                },
            }),
        };
        let expected = project_statement(ProjectInputPlan::LeftOuterJoin(Box::new(
            LeftOuterJoinPlan {
                input: LeftOuterJoinInputPlan::NestedLoop(NestedLoopJoinPlan {
                    input: NestedLoopJoinInputPlan::InnerJoin(Box::new(first_join)),
                    right: SourcePlan::Table(TableSourcePlan {
                        name: "C".to_owned(),
                        alias: None,
                        access: TableAccessPlan::FullScan,
                    }),
                }),
            },
        )));

        assert_eq!(actual, expected);
    }

    #[test]
    fn query_plan_wraps_only_present_terminal_stages() {
        assert!(matches!(
            statement_plan("SELECT * FROM Item"),
            StatementPlan::Query(QueryPlan::Project(_))
        ));
        assert!(matches!(
            statement_plan("SELECT * FROM Item ORDER BY id"),
            StatementPlan::Query(QueryPlan::SelectOrderBy(_))
        ));
        assert!(matches!(
            statement_plan("SELECT * FROM Item LIMIT 3"),
            StatementPlan::Query(QueryPlan::Limit(LimitPlan {
                input: LimitInputPlan::Project(_),
                ..
            }))
        ));
        assert!(matches!(
            statement_plan("SELECT * FROM Item OFFSET 2"),
            StatementPlan::Query(QueryPlan::Offset(OffsetPlan {
                input: OffsetInputPlan::Project(_),
                ..
            }))
        ));
        assert!(matches!(
            statement_plan("SELECT * FROM Item ORDER BY id OFFSET 2"),
            StatementPlan::Query(QueryPlan::Offset(OffsetPlan {
                input: OffsetInputPlan::SelectOrderBy(_),
                ..
            }))
        ));
        assert!(matches!(
            statement_plan("SELECT * FROM Item ORDER BY id LIMIT 3"),
            StatementPlan::Query(QueryPlan::Limit(LimitPlan {
                input: LimitInputPlan::SelectOrderBy(_),
                ..
            }))
        ));
        assert!(matches!(
            statement_plan("SELECT * FROM Item LIMIT 3 OFFSET 2"),
            StatementPlan::Query(QueryPlan::Limit(LimitPlan {
                count: ExprPlan::Literal(Literal::Number(limit)),
                input: LimitInputPlan::Offset(OffsetPlan {
                    count: ExprPlan::Literal(Literal::Number(offset)),
                    input: OffsetInputPlan::Project(_),
                }),
            })) if limit == 3 && offset == 2
        ));
        assert!(matches!(
            statement_plan("SELECT * FROM Item ORDER BY id LIMIT 3 OFFSET 2"),
            StatementPlan::Query(QueryPlan::Limit(LimitPlan {
                input: LimitInputPlan::Offset(OffsetPlan {
                    input: OffsetInputPlan::SelectOrderBy(SelectOrderByPlan { .. }),
                    ..
                }),
                ..
            }))
        ));
    }

    #[test]
    fn query_plan_places_select_distinct_after_order_by() {
        assert!(matches!(
            statement_plan("SELECT DISTINCT * FROM Item"),
            StatementPlan::Query(QueryPlan::Distinct(DistinctPlan {
                input: DistinctInputPlan::Project(_),
            }))
        ));
        assert!(matches!(
            statement_plan("SELECT DISTINCT * FROM Item ORDER BY id"),
            StatementPlan::Query(QueryPlan::Distinct(DistinctPlan {
                input: DistinctInputPlan::SelectOrderBy(_),
            }))
        ));
        assert!(matches!(
            statement_plan("SELECT DISTINCT * FROM Item OFFSET 2"),
            StatementPlan::Query(QueryPlan::Offset(OffsetPlan {
                input: OffsetInputPlan::Distinct(DistinctPlan {
                    input: DistinctInputPlan::Project(_),
                }),
                ..
            }))
        ));
        assert!(matches!(
            statement_plan("SELECT DISTINCT * FROM Item ORDER BY id OFFSET 2"),
            StatementPlan::Query(QueryPlan::Offset(OffsetPlan {
                input: OffsetInputPlan::Distinct(DistinctPlan {
                    input: DistinctInputPlan::SelectOrderBy(_),
                }),
                ..
            }))
        ));
        assert!(matches!(
            statement_plan("SELECT DISTINCT * FROM Item LIMIT 3"),
            StatementPlan::Query(QueryPlan::Limit(LimitPlan {
                input: LimitInputPlan::Distinct(DistinctPlan {
                    input: DistinctInputPlan::Project(_),
                }),
                ..
            }))
        ));
        assert!(matches!(
            statement_plan("SELECT DISTINCT * FROM Item ORDER BY id LIMIT 3"),
            StatementPlan::Query(QueryPlan::Limit(LimitPlan {
                input: LimitInputPlan::Distinct(DistinctPlan {
                    input: DistinctInputPlan::SelectOrderBy(_),
                }),
                ..
            }))
        ));
        assert!(matches!(
            statement_plan("SELECT DISTINCT * FROM Item LIMIT 3 OFFSET 2"),
            StatementPlan::Query(QueryPlan::Limit(LimitPlan {
                input: LimitInputPlan::Offset(OffsetPlan {
                    input: OffsetInputPlan::Distinct(DistinctPlan {
                        input: DistinctInputPlan::Project(_),
                    }),
                    ..
                }),
                ..
            }))
        ));
        assert!(matches!(
            statement_plan("SELECT DISTINCT * FROM Item ORDER BY id LIMIT 3 OFFSET 2"),
            StatementPlan::Query(QueryPlan::Limit(LimitPlan {
                input: LimitInputPlan::Offset(OffsetPlan {
                    input: OffsetInputPlan::Distinct(DistinctPlan {
                        input: DistinctInputPlan::SelectOrderBy(_),
                    }),
                    ..
                }),
                ..
            }))
        ));
    }

    #[test]
    fn query_plan_preserves_values_terminal_stage_relations() {
        assert!(matches!(
            statement_plan("VALUES (1)"),
            StatementPlan::Query(QueryPlan::Values(_))
        ));
        assert!(matches!(
            statement_plan("VALUES (1) ORDER BY column1"),
            StatementPlan::Query(QueryPlan::ValuesOrderBy(_))
        ));
        assert!(matches!(
            statement_plan("VALUES (1) OFFSET 2"),
            StatementPlan::Query(QueryPlan::Offset(OffsetPlan {
                input: OffsetInputPlan::Values(_),
                ..
            }))
        ));
        assert!(matches!(
            statement_plan("VALUES (1) ORDER BY column1 OFFSET 2"),
            StatementPlan::Query(QueryPlan::Offset(OffsetPlan {
                input: OffsetInputPlan::ValuesOrderBy(_),
                ..
            }))
        ));
        assert!(matches!(
            statement_plan("VALUES (1) LIMIT 3"),
            StatementPlan::Query(QueryPlan::Limit(LimitPlan {
                input: LimitInputPlan::Values(_),
                ..
            }))
        ));
        assert!(matches!(
            statement_plan("VALUES (1) ORDER BY column1 LIMIT 3"),
            StatementPlan::Query(QueryPlan::Limit(LimitPlan {
                input: LimitInputPlan::ValuesOrderBy(_),
                ..
            }))
        ));
        assert!(matches!(
            statement_plan("VALUES (1) LIMIT 3 OFFSET 2"),
            StatementPlan::Query(QueryPlan::Limit(LimitPlan {
                input: LimitInputPlan::Offset(OffsetPlan {
                    input: OffsetInputPlan::Values(_),
                    ..
                }),
                ..
            }))
        ));
        assert!(matches!(
            statement_plan("VALUES (1) ORDER BY column1 LIMIT 3 OFFSET 2"),
            StatementPlan::Query(QueryPlan::Limit(LimitPlan {
                input: LimitInputPlan::Offset(OffsetPlan {
                    input: OffsetInputPlan::ValuesOrderBy(ValuesOrderByPlan { .. }),
                    ..
                }),
                ..
            }))
        ));
    }
}
