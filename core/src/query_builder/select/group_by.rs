use {
    super::{
        BuildAggregationInputPlan, BuildAggregationPlan, BuildProjectInputPlan, BuildSelect,
        DistinctNode,
    },
    crate::{
        ast::Select,
        plan::{AggregationPlan, ProjectInputPlan},
        query_builder::{
            ExprList, ExprNode, FilterNode, HavingNode, InnerHashJoinNode, InnerJoinConditionNode,
            InnerNestedLoopJoinNode, LeftOuterHashJoinNode, LeftOuterJoinConditionNode,
            LeftOuterNestedLoopJoinNode, LimitNode, OffsetNode, OrderByExprList, ProjectNode,
            QueryNode, SelectItemList, SelectNode, SelectOrderByNode, SourceNode,
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
    Filter(FilterNode<'a>),
}

impl BuildAggregationInputPlan for PrevNode<'_> {
    fn build_aggregation_input_plan(self) -> Result<crate::plan::AggregationInputPlan> {
        match self {
            Self::Select(node) => node.build_aggregation_input_plan(),
            Self::InnerNestedLoop(node) => node.build_aggregation_input_plan(),
            Self::LeftOuterNestedLoop(node) => node.build_aggregation_input_plan(),
            Self::InnerHash(node) => node.build_aggregation_input_plan(),
            Self::LeftOuterHash(node) => node.build_aggregation_input_plan(),
            Self::InnerCondition(node) => node.build_aggregation_input_plan(),
            Self::LeftOuterCondition(node) => node.build_aggregation_input_plan(),
            Self::Filter(node) => node.build_aggregation_input_plan(),
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
            Self::Filter(node) => node.build_select(),
        }
    }
}

impl<'a> From<SelectNode<'a>> for PrevNode<'a> {
    fn from(node: SelectNode<'a>) -> Self {
        PrevNode::Select(node)
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

impl<'a> From<FilterNode<'a>> for PrevNode<'a> {
    fn from(node: FilterNode<'a>) -> Self {
        PrevNode::Filter(node)
    }
}

#[derive(Clone, Debug)]
pub struct GroupByNode<'a> {
    prev_node: PrevNode<'a>,
    expr_list: ExprList<'a>,
}

impl<'a> GroupByNode<'a> {
    pub(super) fn new<N: Into<PrevNode<'a>>, T: Into<ExprList<'a>>>(
        prev_node: N,
        expr_list: T,
    ) -> Self {
        Self {
            prev_node: prev_node.into(),
            expr_list: expr_list.into(),
        }
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

    pub fn project<T: Into<SelectItemList<'a>>>(self, select_items: T) -> ProjectNode<'a> {
        ProjectNode::new(self, select_items)
    }

    pub fn order_by<T: Into<OrderByExprList<'a>>>(self, expr_list: T) -> SelectOrderByNode<'a> {
        SelectOrderByNode::new(self, expr_list)
    }

    pub fn distinct(self) -> DistinctNode<'a> {
        DistinctNode::new(self)
    }

    pub fn alias_as(self, table_alias: &'a str) -> SourceNode<'a> {
        QueryNode::GroupByNode(self).alias_as(table_alias)
    }
}

impl BuildAggregationPlan for GroupByNode<'_> {
    fn build_aggregation_plan(self) -> Result<AggregationPlan> {
        Ok(AggregationPlan {
            input: self.prev_node.build_aggregation_input_plan()?,
            group_by: self.expr_list.build_exprs_plan()?,
            aggregate_slots: Vec::new(),
        })
    }
}

impl BuildProjectInputPlan for GroupByNode<'_> {
    fn build_project_input_plan(self) -> Result<ProjectInputPlan> {
        self.build_aggregation_plan()
            .map(ProjectInputPlan::Aggregation)
    }
}

impl BuildSelect for GroupByNode<'_> {
    fn build_select(self) -> Result<Select> {
        let mut select = self.prev_node.build_select()?;
        select.group_by = self.expr_list.build_exprs()?;

        Ok(select)
    }
}

#[cfg(test)]
mod tests {
    use {
        crate::{
            plan::{
                AggregationInputPlan, AggregationPlan, HashJoinInputPlan, HashJoinPlan,
                InnerJoinInputPlan, InnerJoinPlan, ProjectInputPlan, ProjectPlan, ProjectionPlan,
                QueryPlan, SourcePlan, StatementPlan, TableAccessPlan, TableSourcePlan,
            },
            query_builder::{Build, SelectItemList, col, table, test_query_builder},
        },
        pretty_assertions::assert_eq,
    };

    #[test]
    fn group_by() {
        // select node -> group by node -> build
        let actual = table("Foo").select().group_by("a");
        let expected = "SELECT * FROM Foo GROUP BY a";
        test_query_builder(actual, expected);

        // inner nested loop join node -> group by node -> build
        let actual = table("Foo").select().join("Bar").group_by("b");
        let expected = "SELECT * FROM Foo JOIN Bar GROUP BY b";
        test_query_builder(actual, expected);

        // inner nested loop join node -> group by node -> build
        let actual = table("Foo").select().join_as("Bar", "B").group_by("b");
        let expected = "SELECT * FROM Foo JOIN Bar AS B GROUP BY b";
        test_query_builder(actual, expected);

        // left outer nested loop join node -> group by node -> build
        let actual = table("Foo").select().left_join("Bar").group_by("b");
        let expected = "SELECT * FROM Foo LEFT JOIN Bar GROUP BY b";
        test_query_builder(actual, expected);

        // left outer nested loop join node -> group by node -> build
        let actual = table("Foo").select().left_join_as("Bar", "B").group_by("b");
        let expected = "SELECT * FROM Foo LEFT JOIN Bar AS B GROUP BY b";
        test_query_builder(actual, expected);

        // inner join condition node -> group by node -> build
        let actual = table("Foo")
            .select()
            .join("Bar")
            .on("Foo.id = Bar.id")
            .group_by("b");
        let expected = "SELECT * FROM Foo JOIN Bar ON Foo.id = Bar.id GROUP BY b";
        test_query_builder(actual, expected);

        // filter node -> group by node -> build
        let actual = table("Bar")
            .select()
            .filter(col("id").is_null())
            .group_by("id, (a + name)");
        let expected = "
                SELECT * FROM Bar
                WHERE id IS NULL
                GROUP BY id, (a + name)
            ";
        test_query_builder(actual, expected);

        // inner hash join node -> group by node -> build
        let actual = table("Player")
            .select()
            .join("PlayerItem")
            .hash_executor("PlayerItem.user_id", "Player.id")
            .group_by("PlayerItem.category")
            .build();
        let expected = {
            let join = InnerJoinPlan {
                input: InnerJoinInputPlan::Hash(HashJoinPlan {
                    input: HashJoinInputPlan::Source(SourcePlan::Table(TableSourcePlan {
                        name: "Player".to_owned(),
                        alias: None,
                        access: TableAccessPlan::FullScan,
                    })),
                    right: SourcePlan::Table(TableSourcePlan {
                        name: "PlayerItem".to_owned(),
                        alias: None,
                        access: TableAccessPlan::FullScan,
                    }),
                    input_key: col("Player.id").build_expr_plan().unwrap(),
                    right_key: col("PlayerItem.user_id").build_expr_plan().unwrap(),
                    right_filter: None,
                }),
            };
            let project = ProjectPlan {
                input: ProjectInputPlan::Aggregation(AggregationPlan {
                    input: AggregationInputPlan::InnerJoin(Box::new(join)),
                    group_by: vec![col("PlayerItem.category").build_expr_plan().unwrap()],
                    aggregate_slots: Vec::new(),
                }),
                projection: ProjectionPlan::SelectItems(
                    SelectItemList::from("*").build_select_items_plan().unwrap(),
                ),
            };

            Ok(StatementPlan::Query(QueryPlan::Project(project)))
        };
        assert_eq!(actual, expected);

        // select -> group by -> derived subquery
        let actual = table("Foo").select().group_by("a").alias_as("Sub").select();
        let expected = "SELECT * FROM (SELECT * FROM Foo GROUP BY a) Sub";
        test_query_builder(actual, expected);
    }
}
