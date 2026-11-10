use {
    super::{inner_hash_join::InnerHashJoinNode, table_factor, table_source_plan},
    crate::{
        ast::{Expr, Join, JoinConstraint, JoinOperator, Select},
        plan::{
            AggregationInputPlan, FilterInputPlan, InnerJoinInputPlan, InnerJoinPlan,
            NestedLoopJoinInputPlan, NestedLoopJoinPlan, ProjectInputPlan,
        },
        query_builder::{
            DistinctNode, ExprList, ExprNode, FilterNode, GroupByNode, HavingNode,
            InnerJoinConditionNode, LeftOuterHashJoinNode, LeftOuterJoinConditionNode,
            LeftOuterNestedLoopJoinNode, LimitNode, OffsetNode, OrderByExprList, ProjectNode,
            QueryNode, SelectItemList, SelectNode, SelectOrderByNode, SourceNode,
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
    fn build_nested_loop_input_plan(self) -> Result<NestedLoopJoinInputPlan> {
        match self {
            Self::Select(node) => node
                .build_source_plan()
                .map(NestedLoopJoinInputPlan::Source),
            Self::InnerNestedLoop(node) => node
                .build_inner_join_plan()
                .map(Box::new)
                .map(NestedLoopJoinInputPlan::InnerJoin),
            Self::LeftOuterNestedLoop(node) => node
                .build_left_outer_join_plan()
                .map(Box::new)
                .map(NestedLoopJoinInputPlan::LeftOuterJoin),
            Self::InnerHash(node) => node
                .build_inner_join_plan()
                .map(Box::new)
                .map(NestedLoopJoinInputPlan::InnerJoin),
            Self::LeftOuterHash(node) => node
                .build_left_outer_join_plan()
                .map(Box::new)
                .map(NestedLoopJoinInputPlan::LeftOuterJoin),
            Self::InnerCondition(node) => node
                .build_inner_join_plan()
                .map(Box::new)
                .map(NestedLoopJoinInputPlan::InnerJoin),
            Self::LeftOuterCondition(node) => node
                .build_left_outer_join_plan()
                .map(Box::new)
                .map(NestedLoopJoinInputPlan::LeftOuterJoin),
        }
    }
}

impl BuildSelect for PrevNode<'_> {
    fn build_select(self) -> Result<Select> {
        match self {
            Self::Select(node) => node.build_select(),
            Self::InnerNestedLoop(node) => node.build_select(),
            Self::LeftOuterNestedLoop(node) => node.build_select(),
            Self::InnerHash(node) => node.build_select(),
            Self::LeftOuterHash(node) => node.build_select(),
            Self::InnerCondition(node) => node.build_select(),
            Self::LeftOuterCondition(node) => node.build_select(),
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
pub struct InnerNestedLoopJoinNode<'a> {
    prev_node: PrevNode<'a>,
    right_name: String,
    right_alias: Option<String>,
}

impl<'a> InnerNestedLoopJoinNode<'a> {
    pub(super) fn new<N: Into<PrevNode<'a>>>(
        prev_node: N,
        name: String,
        alias: Option<String>,
    ) -> Self {
        Self {
            prev_node: prev_node.into(),
            right_name: name,
            right_alias: alias,
        }
    }

    pub(in crate::query_builder::select) fn from_select(
        prev_node: SelectNode<'a>,
        name: String,
        alias: Option<String>,
    ) -> Self {
        Self::new(prev_node, name, alias)
    }

    pub fn on<T: Into<ExprNode<'a>>>(self, expr: T) -> InnerJoinConditionNode<'a> {
        InnerJoinConditionNode::new(self, expr)
    }

    pub fn hash_executor<T: Into<ExprNode<'a>>, U: Into<ExprNode<'a>>>(
        self,
        right_key: T,
        input_key: U,
    ) -> InnerHashJoinNode<'a> {
        let Self {
            prev_node,
            right_name,
            right_alias,
        } = self;
        let right_key = right_key.into();
        let input_key = input_key.into();

        match prev_node {
            PrevNode::Select(node) => {
                InnerHashJoinNode::new(node, right_name, right_alias, right_key, input_key)
            }
            PrevNode::InnerNestedLoop(node) => {
                InnerHashJoinNode::new(*node, right_name, right_alias, right_key, input_key)
            }
            PrevNode::LeftOuterNestedLoop(node) => {
                InnerHashJoinNode::new(*node, right_name, right_alias, right_key, input_key)
            }
            PrevNode::InnerHash(node) => {
                InnerHashJoinNode::new(*node, right_name, right_alias, right_key, input_key)
            }
            PrevNode::LeftOuterHash(node) => {
                InnerHashJoinNode::new(*node, right_name, right_alias, right_key, input_key)
            }
            PrevNode::InnerCondition(node) => {
                InnerHashJoinNode::new(*node, right_name, right_alias, right_key, input_key)
            }
            PrevNode::LeftOuterCondition(node) => {
                InnerHashJoinNode::new(*node, right_name, right_alias, right_key, input_key)
            }
        }
    }

    pub(super) fn build_nested_loop_join_plan(self) -> Result<NestedLoopJoinPlan> {
        Ok(NestedLoopJoinPlan {
            input: self.prev_node.build_nested_loop_input_plan()?,
            right: table_source_plan(self.right_name, self.right_alias),
        })
    }

    pub(super) fn build_inner_join_plan(self) -> Result<InnerJoinPlan> {
        self.build_nested_loop_join_plan()
            .map(InnerJoinInputPlan::NestedLoop)
            .map(|input| InnerJoinPlan { input })
    }

    pub(super) fn build_select_with_condition(self, expr: Option<Expr>) -> Result<Select> {
        let relation = table_factor(self.right_name, self.right_alias);
        let mut select = self.prev_node.build_select()?;
        let constraint = expr.map_or(JoinConstraint::None, JoinConstraint::On);
        select.from.joins.push(Join {
            relation,
            join_operator: JoinOperator::Inner(constraint),
        });

        Ok(select)
    }

    #[must_use]
    pub fn join(self, table_name: &str) -> InnerNestedLoopJoinNode<'a> {
        InnerNestedLoopJoinNode::new(self, table_name.to_owned(), None)
    }

    #[must_use]
    pub fn join_as(self, table_name: &str, alias: &str) -> InnerNestedLoopJoinNode<'a> {
        InnerNestedLoopJoinNode::new(self, table_name.to_owned(), Some(alias.to_owned()))
    }

    #[must_use]
    pub fn left_join(self, table_name: &str) -> LeftOuterNestedLoopJoinNode<'a> {
        LeftOuterNestedLoopJoinNode::new(self, table_name.to_owned(), None)
    }

    #[must_use]
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
        QueryNode::InnerNestedLoopJoinNode(self).alias_as(table_alias)
    }
}

impl BuildFilterInputPlan for InnerNestedLoopJoinNode<'_> {
    fn build_filter_input_plan(self) -> Result<FilterInputPlan> {
        self.build_inner_join_plan()
            .map(Box::new)
            .map(FilterInputPlan::InnerJoin)
    }
}

impl BuildAggregationInputPlan for InnerNestedLoopJoinNode<'_> {
    fn build_aggregation_input_plan(self) -> Result<AggregationInputPlan> {
        self.build_inner_join_plan()
            .map(Box::new)
            .map(AggregationInputPlan::InnerJoin)
    }
}

impl BuildProjectInputPlan for InnerNestedLoopJoinNode<'_> {
    fn build_project_input_plan(self) -> Result<ProjectInputPlan> {
        self.build_inner_join_plan()
            .map(Box::new)
            .map(ProjectInputPlan::InnerJoin)
    }
}

impl BuildSelect for InnerNestedLoopJoinNode<'_> {
    fn build_select(self) -> Result<Select> {
        self.build_select_with_condition(None)
    }
}

#[cfg(test)]
mod tests {
    use {
        crate::{
            plan::{
                HashJoinInputPlan, HashJoinPlan, InnerJoinInputPlan, InnerJoinPlan,
                JoinConditionInputPlan, JoinConditionPlan, LeftOuterJoinInputPlan,
                LeftOuterJoinPlan, NestedLoopJoinInputPlan, NestedLoopJoinPlan, SourcePlan,
                TableAccessPlan, TableAliasPlan, TableSourcePlan,
            },
            query_builder::{
                QueryBuilderError, col, expr, select::BuildQuery, table, test_query_builder,
            },
            result::Error,
        },
        pretty_assertions::assert_eq,
    };

    #[test]
    fn plan() {
        let actual = table("A")
            .select()
            .join_as("B", "b")
            .build_inner_join_plan()
            .unwrap();
        let expected = InnerJoinPlan {
            input: InnerJoinInputPlan::NestedLoop(NestedLoopJoinPlan {
                input: NestedLoopJoinInputPlan::Source(SourcePlan::Table(TableSourcePlan {
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
            }),
        };
        assert_eq!(actual, expected);
    }

    #[test]
    fn successors() {
        let actual = table("A").select().join("B").join("C");
        let expected = "SELECT * FROM A JOIN B JOIN C";
        test_query_builder(actual, expected);

        let actual = table("A").select().join("B").join_as("C", "c");
        let expected = "SELECT * FROM A JOIN B JOIN C AS c";
        test_query_builder(actual, expected);

        let actual = table("A").select().join("B").left_join("C");
        let expected = "SELECT * FROM A JOIN B LEFT JOIN C";
        test_query_builder(actual, expected);

        let actual = table("A").select().join("B").left_join_as("C", "c");
        let expected = "SELECT * FROM A JOIN B LEFT JOIN C AS c";
        test_query_builder(actual, expected);

        let actual = table("A").select().join("B").project("A.id");
        let expected = "SELECT A.id FROM A JOIN B";
        test_query_builder(actual, expected);

        let actual = table("A").select().join("B").group_by("A.id");
        let expected = "SELECT * FROM A JOIN B GROUP BY A.id";
        test_query_builder(actual, expected);

        let actual = table("A").select().join("B").having("TRUE");
        let expected = "SELECT * FROM A JOIN B HAVING TRUE";
        test_query_builder(actual, expected);

        let actual = table("A").select().join("B").offset(1);
        let expected = "SELECT * FROM A JOIN B OFFSET 1";
        test_query_builder(actual, expected);

        let actual = table("A").select().join("B").limit(1);
        let expected = "SELECT * FROM A JOIN B LIMIT 1";
        test_query_builder(actual, expected);

        let actual = table("A").select().join("B").filter("A.id > 0");
        let expected = "SELECT * FROM A JOIN B WHERE A.id > 0";
        test_query_builder(actual, expected);

        let actual = table("A").select().join("B").order_by("A.id");
        let expected = "SELECT * FROM A JOIN B ORDER BY A.id";
        test_query_builder(actual, expected);

        let actual = table("A").select().join("B").distinct();
        let expected = "SELECT DISTINCT * FROM A JOIN B";
        test_query_builder(actual, expected);

        let actual = table("A").select().join("B").alias_as("Joined").select();
        let expected = "SELECT * FROM (SELECT * FROM A JOIN B) Joined";
        test_query_builder(actual, expected);
    }

    #[test]
    fn completed_join_predecessors() {
        let actual = table("A")
            .select()
            .join("B")
            .join("C")
            .build_inner_join_plan()
            .unwrap();
        let expected = InnerJoinPlan {
            input: InnerJoinInputPlan::NestedLoop(NestedLoopJoinPlan {
                input: NestedLoopJoinInputPlan::InnerJoin(Box::new(InnerJoinPlan {
                    input: InnerJoinInputPlan::NestedLoop(NestedLoopJoinPlan {
                        input: NestedLoopJoinInputPlan::Source(SourcePlan::Table(
                            TableSourcePlan {
                                name: "A".to_owned(),
                                alias: None,
                                access: TableAccessPlan::FullScan,
                            },
                        )),
                        right: SourcePlan::Table(TableSourcePlan {
                            name: "B".to_owned(),
                            alias: None,
                            access: TableAccessPlan::FullScan,
                        }),
                    }),
                })),
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
            .left_join("B")
            .join("C")
            .build_inner_join_plan()
            .unwrap();
        let expected = InnerJoinPlan {
            input: InnerJoinInputPlan::NestedLoop(NestedLoopJoinPlan {
                input: NestedLoopJoinInputPlan::LeftOuterJoin(Box::new(LeftOuterJoinPlan {
                    input: LeftOuterJoinInputPlan::NestedLoop(NestedLoopJoinPlan {
                        input: NestedLoopJoinInputPlan::Source(SourcePlan::Table(
                            TableSourcePlan {
                                name: "A".to_owned(),
                                alias: None,
                                access: TableAccessPlan::FullScan,
                            },
                        )),
                        right: SourcePlan::Table(TableSourcePlan {
                            name: "B".to_owned(),
                            alias: None,
                            access: TableAccessPlan::FullScan,
                        }),
                    }),
                })),
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
            .join("C")
            .build_inner_join_plan()
            .unwrap();
        let expected = InnerJoinPlan {
            input: InnerJoinInputPlan::NestedLoop(NestedLoopJoinPlan {
                input: NestedLoopJoinInputPlan::InnerJoin(Box::new(InnerJoinPlan {
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
                })),
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
            .left_join("B")
            .hash_executor("B.id", "A.id")
            .join("C")
            .build_inner_join_plan()
            .unwrap();
        let expected = InnerJoinPlan {
            input: InnerJoinInputPlan::NestedLoop(NestedLoopJoinPlan {
                input: NestedLoopJoinInputPlan::LeftOuterJoin(Box::new(LeftOuterJoinPlan {
                    input: LeftOuterJoinInputPlan::Hash(HashJoinPlan {
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
                })),
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
            .on("A.id = B.id")
            .join("C")
            .build_inner_join_plan()
            .unwrap();
        let expected = InnerJoinPlan {
            input: InnerJoinInputPlan::NestedLoop(NestedLoopJoinPlan {
                input: NestedLoopJoinInputPlan::InnerJoin(Box::new(InnerJoinPlan {
                    input: InnerJoinInputPlan::Condition(JoinConditionPlan {
                        input: JoinConditionInputPlan::NestedLoop(NestedLoopJoinPlan {
                            input: NestedLoopJoinInputPlan::Source(SourcePlan::Table(
                                TableSourcePlan {
                                    name: "A".to_owned(),
                                    alias: None,
                                    access: TableAccessPlan::FullScan,
                                },
                            )),
                            right: SourcePlan::Table(TableSourcePlan {
                                name: "B".to_owned(),
                                alias: None,
                                access: TableAccessPlan::FullScan,
                            }),
                        }),
                        expr: expr("A.id = B.id").build_expr_plan().unwrap(),
                    }),
                })),
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
            .left_join("B")
            .on("A.id = B.id")
            .join("C")
            .build_inner_join_plan()
            .unwrap();
        let expected = InnerJoinPlan {
            input: InnerJoinInputPlan::NestedLoop(NestedLoopJoinPlan {
                input: NestedLoopJoinInputPlan::LeftOuterJoin(Box::new(LeftOuterJoinPlan {
                    input: LeftOuterJoinInputPlan::Condition(JoinConditionPlan {
                        input: JoinConditionInputPlan::NestedLoop(NestedLoopJoinPlan {
                            input: NestedLoopJoinInputPlan::Source(SourcePlan::Table(
                                TableSourcePlan {
                                    name: "A".to_owned(),
                                    alias: None,
                                    access: TableAccessPlan::FullScan,
                                },
                            )),
                            right: SourcePlan::Table(TableSourcePlan {
                                name: "B".to_owned(),
                                alias: None,
                                access: TableAccessPlan::FullScan,
                            }),
                        }),
                        expr: expr("A.id = B.id").build_expr_plan().unwrap(),
                    }),
                })),
                right: SourcePlan::Table(TableSourcePlan {
                    name: "C".to_owned(),
                    alias: None,
                    access: TableAccessPlan::FullScan,
                }),
            }),
        };
        assert_eq!(actual, expected);
    }

    #[test]
    fn completed_joins_are_hash_inputs() {
        let actual = table("A")
            .select()
            .join("B")
            .join("C")
            .hash_executor("C.id", "A.id")
            .build_hash_join_plan()
            .unwrap();
        let expected = HashJoinPlan {
            input: HashJoinInputPlan::InnerJoin(Box::new(InnerJoinPlan {
                input: InnerJoinInputPlan::NestedLoop(NestedLoopJoinPlan {
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
            })),
            right: SourcePlan::Table(TableSourcePlan {
                name: "C".to_owned(),
                alias: None,
                access: TableAccessPlan::FullScan,
            }),
            input_key: col("A.id").build_expr_plan().unwrap(),
            right_key: col("C.id").build_expr_plan().unwrap(),
            right_filter: None,
        };
        assert_eq!(actual, expected);

        let actual = table("A")
            .select()
            .left_join("B")
            .join("C")
            .hash_executor("C.id", "A.id")
            .build_hash_join_plan()
            .unwrap();
        let expected = HashJoinPlan {
            input: HashJoinInputPlan::LeftOuterJoin(Box::new(LeftOuterJoinPlan {
                input: LeftOuterJoinInputPlan::NestedLoop(NestedLoopJoinPlan {
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
            })),
            right: SourcePlan::Table(TableSourcePlan {
                name: "C".to_owned(),
                alias: None,
                access: TableAccessPlan::FullScan,
            }),
            input_key: col("A.id").build_expr_plan().unwrap(),
            right_key: col("C.id").build_expr_plan().unwrap(),
            right_filter: None,
        };
        assert_eq!(actual, expected);

        let actual = table("A")
            .select()
            .join("B")
            .hash_executor("B.id", "A.id")
            .join("C")
            .hash_executor("C.id", "A.id")
            .build_hash_join_plan()
            .unwrap();
        let expected = HashJoinPlan {
            input: HashJoinInputPlan::InnerJoin(Box::new(InnerJoinPlan {
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
            })),
            right: SourcePlan::Table(TableSourcePlan {
                name: "C".to_owned(),
                alias: None,
                access: TableAccessPlan::FullScan,
            }),
            input_key: col("A.id").build_expr_plan().unwrap(),
            right_key: col("C.id").build_expr_plan().unwrap(),
            right_filter: None,
        };
        assert_eq!(actual, expected);

        let actual = table("A")
            .select()
            .left_join("B")
            .hash_executor("B.id", "A.id")
            .join("C")
            .hash_executor("C.id", "A.id")
            .build_hash_join_plan()
            .unwrap();
        let expected = HashJoinPlan {
            input: HashJoinInputPlan::LeftOuterJoin(Box::new(LeftOuterJoinPlan {
                input: LeftOuterJoinInputPlan::Hash(HashJoinPlan {
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
            })),
            right: SourcePlan::Table(TableSourcePlan {
                name: "C".to_owned(),
                alias: None,
                access: TableAccessPlan::FullScan,
            }),
            input_key: col("A.id").build_expr_plan().unwrap(),
            right_key: col("C.id").build_expr_plan().unwrap(),
            right_filter: None,
        };
        assert_eq!(actual, expected);

        let actual = table("A")
            .select()
            .join("B")
            .on("A.id = B.id")
            .join("C")
            .hash_executor("C.id", "A.id")
            .build_hash_join_plan()
            .unwrap();
        let expected = HashJoinPlan {
            input: HashJoinInputPlan::InnerJoin(Box::new(InnerJoinPlan {
                input: InnerJoinInputPlan::Condition(JoinConditionPlan {
                    input: JoinConditionInputPlan::NestedLoop(NestedLoopJoinPlan {
                        input: NestedLoopJoinInputPlan::Source(SourcePlan::Table(
                            TableSourcePlan {
                                name: "A".to_owned(),
                                alias: None,
                                access: TableAccessPlan::FullScan,
                            },
                        )),
                        right: SourcePlan::Table(TableSourcePlan {
                            name: "B".to_owned(),
                            alias: None,
                            access: TableAccessPlan::FullScan,
                        }),
                    }),
                    expr: expr("A.id = B.id").build_expr_plan().unwrap(),
                }),
            })),
            right: SourcePlan::Table(TableSourcePlan {
                name: "C".to_owned(),
                alias: None,
                access: TableAccessPlan::FullScan,
            }),
            input_key: col("A.id").build_expr_plan().unwrap(),
            right_key: col("C.id").build_expr_plan().unwrap(),
            right_filter: None,
        };
        assert_eq!(actual, expected);

        let actual = table("A")
            .select()
            .left_join("B")
            .on("A.id = B.id")
            .join("C")
            .hash_executor("C.id", "A.id")
            .build_hash_join_plan()
            .unwrap();
        let expected = HashJoinPlan {
            input: HashJoinInputPlan::LeftOuterJoin(Box::new(LeftOuterJoinPlan {
                input: LeftOuterJoinInputPlan::Condition(JoinConditionPlan {
                    input: JoinConditionInputPlan::NestedLoop(NestedLoopJoinPlan {
                        input: NestedLoopJoinInputPlan::Source(SourcePlan::Table(
                            TableSourcePlan {
                                name: "A".to_owned(),
                                alias: None,
                                access: TableAccessPlan::FullScan,
                            },
                        )),
                        right: SourcePlan::Table(TableSourcePlan {
                            name: "B".to_owned(),
                            alias: None,
                            access: TableAccessPlan::FullScan,
                        }),
                    }),
                    expr: expr("A.id = B.id").build_expr_plan().unwrap(),
                }),
            })),
            right: SourcePlan::Table(TableSourcePlan {
                name: "C".to_owned(),
                alias: None,
                access: TableAccessPlan::FullScan,
            }),
            input_key: col("A.id").build_expr_plan().unwrap(),
            right_key: col("C.id").build_expr_plan().unwrap(),
            right_filter: None,
        };
        assert_eq!(actual, expected);
    }

    #[test]
    fn hash_predecessors_preserve_ast_error() {
        let actual = table("A")
            .select()
            .join("B")
            .hash_executor("B.id", "A.id")
            .join("C")
            .build_query();
        let expected = Err(Error::QueryBuilder(
            QueryBuilderError::HashJoinExecutorRequiresPlan,
        ));
        assert_eq!(actual, expected);

        let actual = table("A")
            .select()
            .left_join("B")
            .hash_executor("B.id", "A.id")
            .join("C")
            .build_query();
        let expected = Err(Error::QueryBuilder(
            QueryBuilderError::HashJoinExecutorRequiresPlan,
        ));
        assert_eq!(actual, expected);
    }
}
