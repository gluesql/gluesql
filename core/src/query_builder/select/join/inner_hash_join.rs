use {
    super::{inner_nested_loop_join::InnerNestedLoopJoinNode, table_source_plan},
    crate::{
        ast::Select,
        plan::{
            AggregationInputPlan, HashJoinInputPlan, HashJoinPlan, InnerJoinInputPlan,
            InnerJoinPlan, ProjectInputPlan,
        },
        query_builder::{
            DistinctNode, ExprList, ExprNode, FilterNode, GroupByNode, HavingNode,
            InnerJoinConditionNode, LeftOuterHashJoinNode, LeftOuterJoinConditionNode,
            LeftOuterNestedLoopJoinNode, LimitNode, OffsetNode, OrderByExprList, ProjectNode,
            QueryBuilderError, QueryNode, SelectItemList, SelectNode, SelectOrderByNode,
            SourceNode,
            select::{
                BuildAggregationInputPlan, BuildFilterInputPlan, BuildProjectInputPlan,
                BuildSelect, BuildSourcePlan,
            },
        },
        result::Result,
    },
};

#[derive(Clone, Debug)]
pub(super) enum PrevNode<'a> {
    Select(SelectNode<'a>),
    InnerNestedLoop(Box<InnerNestedLoopJoinNode<'a>>),
    LeftOuterNestedLoop(Box<LeftOuterNestedLoopJoinNode<'a>>),
    InnerHash(Box<InnerHashJoinNode<'a>>),
    LeftOuterHash(Box<LeftOuterHashJoinNode<'a>>),
    InnerCondition(Box<InnerJoinConditionNode<'a>>),
    LeftOuterCondition(Box<LeftOuterJoinConditionNode<'a>>),
}

impl PrevNode<'_> {
    fn build_hash_join_input_plan(self) -> Result<HashJoinInputPlan> {
        match self {
            Self::Select(node) => node.build_source_plan().map(HashJoinInputPlan::Source),
            Self::InnerNestedLoop(node) => node
                .build_inner_join_plan()
                .map(Box::new)
                .map(HashJoinInputPlan::InnerJoin),
            Self::LeftOuterNestedLoop(node) => node
                .build_left_outer_join_plan()
                .map(Box::new)
                .map(HashJoinInputPlan::LeftOuterJoin),
            Self::InnerHash(node) => node
                .build_inner_join_plan()
                .map(Box::new)
                .map(HashJoinInputPlan::InnerJoin),
            Self::LeftOuterHash(node) => node
                .build_left_outer_join_plan()
                .map(Box::new)
                .map(HashJoinInputPlan::LeftOuterJoin),
            Self::InnerCondition(node) => node
                .build_inner_join_plan()
                .map(Box::new)
                .map(HashJoinInputPlan::InnerJoin),
            Self::LeftOuterCondition(node) => node
                .build_left_outer_join_plan()
                .map(Box::new)
                .map(HashJoinInputPlan::LeftOuterJoin),
        }
    }
}

impl<'a> From<SelectNode<'a>> for PrevNode<'a> {
    fn from(node: SelectNode<'a>) -> Self {
        Self::Select(node)
    }
}

impl<'a> From<InnerNestedLoopJoinNode<'a>> for PrevNode<'a> {
    fn from(node: InnerNestedLoopJoinNode<'a>) -> Self {
        Self::InnerNestedLoop(Box::new(node))
    }
}

impl<'a> From<LeftOuterNestedLoopJoinNode<'a>> for PrevNode<'a> {
    fn from(node: LeftOuterNestedLoopJoinNode<'a>) -> Self {
        Self::LeftOuterNestedLoop(Box::new(node))
    }
}

impl<'a> From<InnerHashJoinNode<'a>> for PrevNode<'a> {
    fn from(node: InnerHashJoinNode<'a>) -> Self {
        Self::InnerHash(Box::new(node))
    }
}

impl<'a> From<LeftOuterHashJoinNode<'a>> for PrevNode<'a> {
    fn from(node: LeftOuterHashJoinNode<'a>) -> Self {
        Self::LeftOuterHash(Box::new(node))
    }
}

impl<'a> From<InnerJoinConditionNode<'a>> for PrevNode<'a> {
    fn from(node: InnerJoinConditionNode<'a>) -> Self {
        Self::InnerCondition(Box::new(node))
    }
}

impl<'a> From<LeftOuterJoinConditionNode<'a>> for PrevNode<'a> {
    fn from(node: LeftOuterJoinConditionNode<'a>) -> Self {
        Self::LeftOuterCondition(Box::new(node))
    }
}

#[derive(Clone, Debug)]
pub struct InnerHashJoinNode<'a> {
    prev_node: PrevNode<'a>,
    right_name: String,
    right_alias: Option<String>,
    right_key: ExprNode<'a>,
    input_key: ExprNode<'a>,
    right_filter: Option<ExprNode<'a>>,
}

impl<'a> InnerHashJoinNode<'a> {
    pub(super) fn new<N: Into<PrevNode<'a>>>(
        prev_node: N,
        right_name: String,
        right_alias: Option<String>,
        right_key: ExprNode<'a>,
        input_key: ExprNode<'a>,
    ) -> Self {
        Self {
            prev_node: prev_node.into(),
            right_name,
            right_alias,
            right_key,
            input_key,
            right_filter: None,
        }
    }

    #[must_use]
    pub fn hash_filter<T: Into<ExprNode<'a>>>(mut self, expr: T) -> Self {
        let expr = expr.into();
        self.right_filter = Some(match self.right_filter {
            Some(right_filter) => right_filter.and(expr),
            None => expr,
        });

        self
    }

    pub fn on<T: Into<ExprNode<'a>>>(self, expr: T) -> InnerJoinConditionNode<'a> {
        InnerJoinConditionNode::new(self, expr)
    }

    pub(super) fn build_hash_join_plan(self) -> Result<HashJoinPlan> {
        Ok(HashJoinPlan {
            input: self.prev_node.build_hash_join_input_plan()?,
            right: table_source_plan(self.right_name, self.right_alias),
            input_key: self.input_key.build_expr_plan()?,
            right_key: self.right_key.build_expr_plan()?,
            right_filter: self
                .right_filter
                .map(ExprNode::build_expr_plan)
                .transpose()?,
        })
    }

    pub(super) fn build_inner_join_plan(self) -> Result<InnerJoinPlan> {
        self.build_hash_join_plan()
            .map(InnerJoinInputPlan::Hash)
            .map(|input| InnerJoinPlan { input })
    }

    pub fn join(self, table_name: &str) -> InnerNestedLoopJoinNode<'a> {
        InnerNestedLoopJoinNode::new(self, table_name.to_owned(), None)
    }

    pub fn join_as(self, table_name: &str, alias: &str) -> InnerNestedLoopJoinNode<'a> {
        InnerNestedLoopJoinNode::new(self, table_name.to_owned(), Some(alias.to_owned()))
    }

    pub fn left_join(self, table_name: &str) -> LeftOuterNestedLoopJoinNode<'a> {
        LeftOuterNestedLoopJoinNode::new(self, table_name.to_owned(), None)
    }

    pub fn left_join_as(self, table_name: &str, alias: &str) -> LeftOuterNestedLoopJoinNode<'a> {
        LeftOuterNestedLoopJoinNode::new(self, table_name.to_owned(), Some(alias.to_owned()))
    }

    pub fn project<T: Into<SelectItemList<'a>>>(self, select_items: T) -> ProjectNode<'a> {
        ProjectNode::new(self, select_items)
    }

    pub fn group_by<T: Into<ExprList<'a>>>(self, expr_list: T) -> GroupByNode<'a> {
        GroupByNode::new(self, expr_list)
    }

    pub fn having<T: Into<ExprNode<'a>>>(self, expr: T) -> HavingNode<'a> {
        HavingNode::new(self, expr)
    }

    pub fn offset<T: Into<ExprNode<'a>>>(self, expr: T) -> OffsetNode<'a> {
        OffsetNode::new(self, expr)
    }

    pub fn limit<T: Into<ExprNode<'a>>>(self, expr: T) -> LimitNode<'a> {
        LimitNode::new(self, expr)
    }

    pub fn filter<T: Into<ExprNode<'a>>>(self, expr: T) -> FilterNode<'a> {
        FilterNode::new(self, expr)
    }

    pub fn order_by<T: Into<OrderByExprList<'a>>>(
        self,
        order_by_exprs: T,
    ) -> SelectOrderByNode<'a> {
        SelectOrderByNode::new(self, order_by_exprs)
    }

    pub fn distinct(self) -> DistinctNode<'a> {
        DistinctNode::new(self)
    }

    pub fn alias_as(self, table_alias: &'a str) -> SourceNode<'a> {
        QueryNode::InnerHashJoinNode(self).alias_as(table_alias)
    }
}

impl BuildFilterInputPlan for InnerHashJoinNode<'_> {
    fn build_filter_input_plan(self) -> Result<crate::plan::FilterInputPlan> {
        self.build_inner_join_plan()
            .map(Box::new)
            .map(crate::plan::FilterInputPlan::InnerJoin)
    }
}

impl BuildAggregationInputPlan for InnerHashJoinNode<'_> {
    fn build_aggregation_input_plan(self) -> Result<AggregationInputPlan> {
        self.build_inner_join_plan()
            .map(Box::new)
            .map(AggregationInputPlan::InnerJoin)
    }
}

impl BuildProjectInputPlan for InnerHashJoinNode<'_> {
    fn build_project_input_plan(self) -> Result<ProjectInputPlan> {
        self.build_inner_join_plan()
            .map(Box::new)
            .map(ProjectInputPlan::InnerJoin)
    }
}

impl BuildSelect for InnerHashJoinNode<'_> {
    fn build_select(self) -> Result<Select> {
        Err(QueryBuilderError::HashJoinExecutorRequiresPlan.into())
    }
}

#[cfg(test)]
mod tests {
    use {
        crate::{
            plan::{
                AggregationInputPlan, AggregationPlan, DerivedSourcePlan, DistinctInputPlan,
                DistinctPlan, FilterInputPlan, FilterPlan, HashJoinInputPlan, HashJoinPlan,
                HavingPlan, InnerJoinInputPlan, InnerJoinPlan, LimitInputPlan, LimitPlan,
                NestedLoopJoinInputPlan, NestedLoopJoinPlan, OffsetInputPlan, OffsetPlan,
                OrderByExprPlan, ProjectInputPlan, ProjectPlan, ProjectionPlan, QueryPlan,
                SelectItemPlan, SelectOrderByPlan, SourcePlan, StatementPlan, TableAccessPlan,
                TableAliasPlan, TableSourcePlan,
            },
            query_builder::{Build, QueryBuilderError, col, expr, num, select::BuildQuery, table},
            result::Error,
        },
        pretty_assertions::assert_eq,
    };

    #[test]
    fn plan() {
        let actual = table("A")
            .select()
            .join_as("B", "b")
            .hash_executor("b.id", "A.id")
            .hash_filter("b.active")
            .hash_filter("b.visible")
            .build_inner_join_plan()
            .unwrap();
        let expected = InnerJoinPlan {
            input: InnerJoinInputPlan::Hash(HashJoinPlan {
                input: HashJoinInputPlan::Source(SourcePlan::Table(TableSourcePlan {
                    name: "A".to_owned(),
                    alias: None,
                    access: TableAccessPlan::FullScan,
                })),
                right: SourcePlan::Table(TableSourcePlan {
                    name: "B".to_owned(),
                    alias: Some(TableAliasPlan {
                        name: "b".to_owned(),
                        columns: Vec::new(),
                    }),
                    access: TableAccessPlan::FullScan,
                }),
                input_key: col("A.id").build_expr_plan().unwrap(),
                right_key: col("b.id").build_expr_plan().unwrap(),
                right_filter: Some(expr("b.active AND b.visible").build_expr_plan().unwrap()),
            }),
        };
        assert_eq!(actual, expected);
    }

    #[test]
    fn join_successors() {
        let expected_input = InnerJoinPlan {
            input: InnerJoinInputPlan::Hash(HashJoinPlan {
                input: HashJoinInputPlan::Source(SourcePlan::Table(TableSourcePlan {
                    name: "A".to_owned(),
                    alias: None,
                    access: TableAccessPlan::FullScan,
                })),
                right: SourcePlan::Table(TableSourcePlan {
                    name: "B".to_owned(),
                    alias: None,
                    access: TableAccessPlan::FullScan,
                }),
                input_key: col("A.id").build_expr_plan().unwrap(),
                right_key: col("B.id").build_expr_plan().unwrap(),
                right_filter: None,
            }),
        };

        let actual = table("A")
            .select()
            .join("B")
            .hash_executor("B.id", "A.id")
            .join("C")
            .build_inner_join_plan()
            .unwrap();
        let expected = InnerJoinPlan {
            input: InnerJoinInputPlan::NestedLoop(NestedLoopJoinPlan {
                input: NestedLoopJoinInputPlan::InnerJoin(Box::new(expected_input.clone())),
                right: SourcePlan::Table(TableSourcePlan {
                    name: "C".to_owned(),
                    alias: None,
                    access: TableAccessPlan::FullScan,
                }),
            }),
        };
        assert_eq!(actual, expected);

        let actual = table("A")
            .select()
            .join("B")
            .hash_executor("B.id", "A.id")
            .join_as("C", "c")
            .build_inner_join_plan()
            .unwrap();
        let expected = InnerJoinPlan {
            input: InnerJoinInputPlan::NestedLoop(NestedLoopJoinPlan {
                input: NestedLoopJoinInputPlan::InnerJoin(Box::new(expected_input.clone())),
                right: SourcePlan::Table(TableSourcePlan {
                    name: "C".to_owned(),
                    alias: Some(TableAliasPlan {
                        name: "c".to_owned(),
                        columns: Vec::new(),
                    }),
                    access: TableAccessPlan::FullScan,
                }),
            }),
        };
        assert_eq!(actual, expected);

        let actual = table("A")
            .select()
            .join("B")
            .hash_executor("B.id", "A.id")
            .left_join("C")
            .build_left_outer_join_plan()
            .unwrap();
        let expected = crate::plan::LeftOuterJoinPlan {
            input: crate::plan::LeftOuterJoinInputPlan::NestedLoop(NestedLoopJoinPlan {
                input: NestedLoopJoinInputPlan::InnerJoin(Box::new(expected_input.clone())),
                right: SourcePlan::Table(TableSourcePlan {
                    name: "C".to_owned(),
                    alias: None,
                    access: TableAccessPlan::FullScan,
                }),
            }),
        };
        assert_eq!(actual, expected);

        let actual = table("A")
            .select()
            .join("B")
            .hash_executor("B.id", "A.id")
            .left_join_as("C", "c")
            .build_left_outer_join_plan()
            .unwrap();
        let expected = crate::plan::LeftOuterJoinPlan {
            input: crate::plan::LeftOuterJoinInputPlan::NestedLoop(NestedLoopJoinPlan {
                input: NestedLoopJoinInputPlan::InnerJoin(Box::new(expected_input)),
                right: SourcePlan::Table(TableSourcePlan {
                    name: "C".to_owned(),
                    alias: Some(TableAliasPlan {
                        name: "c".to_owned(),
                        columns: Vec::new(),
                    }),
                    access: TableAccessPlan::FullScan,
                }),
            }),
        };
        assert_eq!(actual, expected);
    }

    #[test]
    fn terminal_successors() {
        let join = InnerJoinPlan {
            input: InnerJoinInputPlan::Hash(HashJoinPlan {
                input: HashJoinInputPlan::Source(SourcePlan::Table(TableSourcePlan {
                    name: "A".to_owned(),
                    alias: None,
                    access: TableAccessPlan::FullScan,
                })),
                right: SourcePlan::Table(TableSourcePlan {
                    name: "B".to_owned(),
                    alias: None,
                    access: TableAccessPlan::FullScan,
                }),
                input_key: col("A.id").build_expr_plan().unwrap(),
                right_key: col("B.id").build_expr_plan().unwrap(),
                right_filter: None,
            }),
        };
        let wildcard = ProjectionPlan::SelectItems(vec![SelectItemPlan::Wildcard]);

        let actual = table("A")
            .select()
            .join("B")
            .hash_executor("B.id", "A.id")
            .project("A.id")
            .build();
        let expected = Ok(StatementPlan::Query(QueryPlan::Project(ProjectPlan {
            input: ProjectInputPlan::InnerJoin(Box::new(join.clone())),
            projection: ProjectionPlan::SelectItems(vec![SelectItemPlan::Expr {
                expr: col("A.id").build_expr_plan().unwrap(),
                label: "id".to_owned(),
            }]),
        })));
        assert_eq!(actual, expected);

        let actual = table("A")
            .select()
            .join("B")
            .hash_executor("B.id", "A.id")
            .group_by("A.id")
            .build();
        let expected = Ok(StatementPlan::Query(QueryPlan::Project(ProjectPlan {
            input: ProjectInputPlan::Aggregation(AggregationPlan {
                input: AggregationInputPlan::InnerJoin(Box::new(join.clone())),
                group_by: vec![col("A.id").build_expr_plan().unwrap()],
                aggregate_slots: Vec::new(),
            }),
            projection: wildcard.clone(),
        })));
        assert_eq!(actual, expected);

        let actual = table("A")
            .select()
            .join("B")
            .hash_executor("B.id", "A.id")
            .having("TRUE")
            .build();
        let expected = Ok(StatementPlan::Query(QueryPlan::Project(ProjectPlan {
            input: ProjectInputPlan::Having(HavingPlan {
                input: AggregationPlan {
                    input: AggregationInputPlan::InnerJoin(Box::new(join.clone())),
                    group_by: Vec::new(),
                    aggregate_slots: Vec::new(),
                },
                expr: expr("TRUE").build_expr_plan().unwrap(),
            }),
            projection: wildcard.clone(),
        })));
        assert_eq!(actual, expected);

        let actual = table("A")
            .select()
            .join("B")
            .hash_executor("B.id", "A.id")
            .filter("A.active")
            .build();
        let expected = Ok(StatementPlan::Query(QueryPlan::Project(ProjectPlan {
            input: ProjectInputPlan::Filter(FilterPlan {
                input: FilterInputPlan::InnerJoin(Box::new(join.clone())),
                expr: col("A.active").build_expr_plan().unwrap(),
            }),
            projection: wildcard.clone(),
        })));
        assert_eq!(actual, expected);

        let project = ProjectPlan {
            input: ProjectInputPlan::InnerJoin(Box::new(join.clone())),
            projection: wildcard.clone(),
        };
        let actual = table("A")
            .select()
            .join("B")
            .hash_executor("B.id", "A.id")
            .order_by("A.id")
            .build();
        let expected = Ok(StatementPlan::Query(QueryPlan::SelectOrderBy(
            SelectOrderByPlan {
                input: project.clone(),
                exprs: vec![OrderByExprPlan {
                    expr: col("A.id").build_expr_plan().unwrap(),
                    asc: None,
                }],
            },
        )));
        assert_eq!(actual, expected);

        let actual = table("A")
            .select()
            .join("B")
            .hash_executor("B.id", "A.id")
            .distinct()
            .build();
        let expected = Ok(StatementPlan::Query(QueryPlan::Distinct(DistinctPlan {
            input: DistinctInputPlan::Project(project.clone()),
        })));
        assert_eq!(actual, expected);

        let actual = table("A")
            .select()
            .join("B")
            .hash_executor("B.id", "A.id")
            .offset(1)
            .build();
        let expected = Ok(StatementPlan::Query(QueryPlan::Offset(OffsetPlan {
            input: OffsetInputPlan::Project(project.clone()),
            count: num(1).build_expr_plan().unwrap(),
        })));
        assert_eq!(actual, expected);

        let actual = table("A")
            .select()
            .join("B")
            .hash_executor("B.id", "A.id")
            .limit(1)
            .build();
        let expected = Ok(StatementPlan::Query(QueryPlan::Limit(LimitPlan {
            input: LimitInputPlan::Project(project),
            count: num(1).build_expr_plan().unwrap(),
        })));
        assert_eq!(actual, expected);

        let actual = table("A")
            .select()
            .join("B")
            .hash_executor("B.id", "A.id")
            .alias_as("Joined")
            .select()
            .build();
        let expected = Ok(StatementPlan::Query(QueryPlan::Project(ProjectPlan {
            input: ProjectInputPlan::Source(SourcePlan::Derived(DerivedSourcePlan {
                query: Box::new(QueryPlan::Project(ProjectPlan {
                    input: ProjectInputPlan::InnerJoin(Box::new(join)),
                    projection: wildcard.clone(),
                })),
                alias: TableAliasPlan {
                    name: "Joined".to_owned(),
                    columns: Vec::new(),
                },
            })),
            projection: wildcard,
        })));
        assert_eq!(actual, expected);
    }

    #[test]
    fn ast_errors() {
        let actual = table("A")
            .select()
            .join("B")
            .hash_executor("B.id", "A.id")
            .build_query();
        let expected = Err(Error::QueryBuilder(
            QueryBuilderError::HashJoinExecutorRequiresPlan,
        ));
        assert_eq!(actual, expected);

        let actual = table("A")
            .select()
            .join("B")
            .hash_executor("B.id", "A.id")
            .project("A.id")
            .build_query();
        let expected = Err(Error::QueryBuilder(
            QueryBuilderError::HashJoinExecutorRequiresPlan,
        ));
        assert_eq!(actual, expected);

        let actual = table("A")
            .select()
            .join("B")
            .hash_executor("B.id", "A.id")
            .group_by("A.id")
            .build_query();
        let expected = Err(Error::QueryBuilder(
            QueryBuilderError::HashJoinExecutorRequiresPlan,
        ));
        assert_eq!(actual, expected);

        let actual = table("A")
            .select()
            .join("B")
            .hash_executor("B.id", "A.id")
            .having("TRUE")
            .build_query();
        let expected = Err(Error::QueryBuilder(
            QueryBuilderError::HashJoinExecutorRequiresPlan,
        ));
        assert_eq!(actual, expected);

        let actual = table("A")
            .select()
            .join("B")
            .hash_executor("B.id", "A.id")
            .filter("A.active")
            .build_query();
        let expected = Err(Error::QueryBuilder(
            QueryBuilderError::HashJoinExecutorRequiresPlan,
        ));
        assert_eq!(actual, expected);

        let actual = table("A")
            .select()
            .join("B")
            .hash_executor("B.id", "A.id")
            .order_by("A.id")
            .build_query();
        let expected = Err(Error::QueryBuilder(
            QueryBuilderError::HashJoinExecutorRequiresPlan,
        ));
        assert_eq!(actual, expected);

        let actual = table("A")
            .select()
            .join("B")
            .hash_executor("B.id", "A.id")
            .distinct()
            .build_query();
        let expected = Err(Error::QueryBuilder(
            QueryBuilderError::HashJoinExecutorRequiresPlan,
        ));
        assert_eq!(actual, expected);

        let actual = table("A")
            .select()
            .join("B")
            .hash_executor("B.id", "A.id")
            .offset(1)
            .build_query();
        let expected = Err(Error::QueryBuilder(
            QueryBuilderError::HashJoinExecutorRequiresPlan,
        ));
        assert_eq!(actual, expected);

        let actual = table("A")
            .select()
            .join("B")
            .hash_executor("B.id", "A.id")
            .limit(1)
            .build_query();
        let expected = Err(Error::QueryBuilder(
            QueryBuilderError::HashJoinExecutorRequiresPlan,
        ));
        assert_eq!(actual, expected);
    }
}
