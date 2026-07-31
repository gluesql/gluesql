use {
    super::{BuildProjectPlan, BuildQuery, BuildQueryPlan, DistinctNode, ValuesNode},
    crate::{
        ast::Query,
        plan::{OffsetInputPlan, OffsetPlan, QueryPlan},
        query_builder::{
            ExprNode, FilterNode, GroupByNode, HavingNode, InnerHashJoinNode,
            InnerJoinConditionNode, InnerNestedLoopJoinNode, LeftOuterHashJoinNode,
            LeftOuterJoinConditionNode, LeftOuterNestedLoopJoinNode, OffsetLimitNode, ProjectNode,
            QueryNode, SelectNode, SelectOrderByNode, SourceNode, ValuesOrderByNode,
        },
        result::Result,
    },
};

#[derive(Clone, Debug)]
pub(super) enum PrevNode<'a> {
    Select(SelectNode<'a>),
    Values(ValuesNode<'a>),
    GroupBy(GroupByNode<'a>),
    Having(HavingNode<'a>),
    InnerNestedLoop(Box<InnerNestedLoopJoinNode<'a>>),
    LeftOuterNestedLoop(Box<LeftOuterNestedLoopJoinNode<'a>>),
    InnerHash(Box<InnerHashJoinNode<'a>>),
    LeftOuterHash(Box<LeftOuterHashJoinNode<'a>>),
    InnerCondition(Box<InnerJoinConditionNode<'a>>),
    LeftOuterCondition(Box<LeftOuterJoinConditionNode<'a>>),
    Filter(FilterNode<'a>),
    SelectOrderBy(SelectOrderByNode<'a>),
    ValuesOrderBy(ValuesOrderByNode<'a>),
    Distinct(DistinctNode<'a>),
    ProjectNode(Box<ProjectNode<'a>>),
}

impl PrevNode<'_> {
    fn build_offset_input_plan(self) -> Result<OffsetInputPlan> {
        match self {
            Self::Select(node) => node.build_project_plan().map(OffsetInputPlan::Project),
            Self::Values(node) => node.build_values_plan().map(OffsetInputPlan::Values),
            Self::GroupBy(node) => node.build_project_plan().map(OffsetInputPlan::Project),
            Self::Having(node) => node.build_project_plan().map(OffsetInputPlan::Project),
            Self::InnerNestedLoop(node) => node.build_project_plan().map(OffsetInputPlan::Project),
            Self::LeftOuterNestedLoop(node) => {
                node.build_project_plan().map(OffsetInputPlan::Project)
            }
            Self::InnerHash(node) => node.build_project_plan().map(OffsetInputPlan::Project),
            Self::LeftOuterHash(node) => node.build_project_plan().map(OffsetInputPlan::Project),
            Self::InnerCondition(node) => node.build_project_plan().map(OffsetInputPlan::Project),
            Self::LeftOuterCondition(node) => {
                node.build_project_plan().map(OffsetInputPlan::Project)
            }
            Self::Filter(node) => node.build_project_plan().map(OffsetInputPlan::Project),
            Self::SelectOrderBy(node) => node
                .build_select_order_by_plan()
                .map(OffsetInputPlan::SelectOrderBy),
            Self::ValuesOrderBy(node) => node
                .build_values_order_by_plan()
                .map(OffsetInputPlan::ValuesOrderBy),
            Self::Distinct(node) => node.build_distinct_plan().map(OffsetInputPlan::Distinct),
            Self::ProjectNode(node) => node.build_project_plan().map(OffsetInputPlan::Project),
        }
    }
}

impl BuildQuery for PrevNode<'_> {
    fn build_query(self) -> Result<Query> {
        match self {
            Self::Select(node) => node.build_query(),
            Self::Values(node) => node.build_query(),
            Self::GroupBy(node) => node.build_query(),
            Self::Having(node) => node.build_query(),
            Self::InnerNestedLoop(node) => node.build_query(),
            Self::LeftOuterNestedLoop(node) => node.build_query(),
            Self::InnerHash(node) => node.build_query(),
            Self::LeftOuterHash(node) => node.build_query(),
            Self::InnerCondition(node) => node.build_query(),
            Self::LeftOuterCondition(node) => node.build_query(),
            Self::Filter(node) => node.build_query(),
            Self::SelectOrderBy(node) => node.build_query(),
            Self::ValuesOrderBy(node) => node.build_query(),
            Self::Distinct(node) => node.build_query(),
            Self::ProjectNode(node) => node.build_query(),
        }
    }
}

impl<'a> From<SelectNode<'a>> for PrevNode<'a> {
    fn from(node: SelectNode<'a>) -> Self {
        PrevNode::Select(node)
    }
}

impl<'a> From<ValuesNode<'a>> for PrevNode<'a> {
    fn from(node: ValuesNode<'a>) -> Self {
        PrevNode::Values(node)
    }
}

impl<'a> From<GroupByNode<'a>> for PrevNode<'a> {
    fn from(node: GroupByNode<'a>) -> Self {
        PrevNode::GroupBy(node)
    }
}

impl<'a> From<HavingNode<'a>> for PrevNode<'a> {
    fn from(node: HavingNode<'a>) -> Self {
        PrevNode::Having(node)
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

impl<'a> From<SelectOrderByNode<'a>> for PrevNode<'a> {
    fn from(node: SelectOrderByNode<'a>) -> Self {
        Self::SelectOrderBy(node)
    }
}

impl<'a> From<ValuesOrderByNode<'a>> for PrevNode<'a> {
    fn from(node: ValuesOrderByNode<'a>) -> Self {
        Self::ValuesOrderBy(node)
    }
}

impl<'a> From<DistinctNode<'a>> for PrevNode<'a> {
    fn from(node: DistinctNode<'a>) -> Self {
        Self::Distinct(node)
    }
}

impl<'a> From<ProjectNode<'a>> for PrevNode<'a> {
    fn from(node: ProjectNode<'a>) -> Self {
        PrevNode::ProjectNode(Box::new(node))
    }
}

#[derive(Clone, Debug)]
pub struct OffsetNode<'a> {
    prev_node: PrevNode<'a>,
    expr: ExprNode<'a>,
}

impl<'a> OffsetNode<'a> {
    pub(super) fn new<N: Into<PrevNode<'a>>, T: Into<ExprNode<'a>>>(prev_node: N, expr: T) -> Self {
        Self {
            prev_node: prev_node.into(),
            expr: expr.into(),
        }
    }

    pub fn limit<T: Into<ExprNode<'a>>>(self, expr: T) -> OffsetLimitNode<'a> {
        OffsetLimitNode::new(self, expr)
    }

    pub fn alias_as(self, table_alias: &'a str) -> SourceNode<'a> {
        QueryNode::OffsetNode(self).alias_as(table_alias)
    }

    pub(super) fn build_offset_plan(self) -> Result<OffsetPlan> {
        let count = self.expr.build_expr_plan()?;
        let input = self.prev_node.build_offset_input_plan()?;

        Ok(OffsetPlan { input, count })
    }
}

impl BuildQueryPlan for OffsetNode<'_> {
    fn build_query_plan(self) -> Result<QueryPlan> {
        self.build_offset_plan().map(QueryPlan::Offset)
    }
}

impl BuildQuery for OffsetNode<'_> {
    fn build_query(self) -> Result<Query> {
        let mut node_data = self.prev_node.build_query()?;
        node_data.offset = Some(self.expr.build_expr()?);

        Ok(node_data)
    }
}

#[cfg(test)]
mod tests {
    use {
        crate::{
            plan::{
                HashJoinInputPlan, HashJoinPlan, InnerJoinInputPlan, InnerJoinPlan,
                OffsetInputPlan, OffsetPlan, ProjectInputPlan, ProjectPlan, ProjectionPlan,
                QueryPlan, SourcePlan, StatementPlan, TableAccessPlan, TableSourcePlan,
            },
            query_builder::{Build, SelectItemList, col, num, table, test_query_builder},
        },
        pretty_assertions::assert_eq,
    };

    #[test]
    fn offset() {
        // select node -> offset node -> build
        let actual = table("Foo").select().offset(10);
        let expected = "SELECT * FROM Foo OFFSET 10";
        test_query_builder(actual, expected);

        // group by node -> offset node -> build
        let actual = table("Foo").select().group_by("id").offset(10);
        let expected = "SELECT * FROM Foo GROUP BY id OFFSET 10";
        test_query_builder(actual, expected);

        // having node -> offset node -> build
        let actual = table("Foo")
            .select()
            .group_by("id")
            .having("id > 10")
            .offset(10);
        let expected = "SELECT * FROM Foo GROUP BY id HAVING id > 10 OFFSET 10";
        test_query_builder(actual, expected);

        // inner nested loop join node -> offset node -> build
        let actual = table("Foo").select().join("Bar").offset(10);
        let expected = "SELECT * FROM Foo JOIN Bar OFFSET 10";
        test_query_builder(actual, expected);

        // inner nested loop join node -> offset node -> build
        let actual = table("Foo").select().join_as("Bar", "B").offset(10);
        let expected = "SELECT * FROM Foo JOIN Bar AS B OFFSET 10";
        test_query_builder(actual, expected);

        // left outer join condition node -> offset node -> build
        let actual = table("Foo")
            .select()
            .left_join("Bar")
            .on("Foo.id = Bar.id")
            .offset(10);
        let expected = "SELECT * FROM Foo LEFT JOIN Bar ON Foo.id = Bar.id OFFSET 10";
        test_query_builder(actual, expected);

        // left outer join condition node -> offset node -> build
        let actual = table("Foo")
            .select()
            .left_join_as("Bar", "B")
            .on("Foo.id = B.id")
            .offset(10);
        let expected = "SELECT * FROM Foo LEFT JOIN Bar AS B ON Foo.id = B.id OFFSET 10";
        test_query_builder(actual, expected);

        // inner join condition node -> offset node -> build
        let actual = table("Foo")
            .select()
            .join("Bar")
            .on("Foo.id = Bar.id")
            .offset(10);
        let expected = "SELECT * FROM Foo JOIN Bar ON Foo.id = Bar.id OFFSET 10";
        test_query_builder(actual, expected);

        // filter node -> offset node -> build
        let actual = table("Bar").select().filter("id > 2").offset(100);
        let expected = "SELECT * FROM Bar WHERE id > 2 OFFSET 100";
        test_query_builder(actual, expected);

        // project node -> offset node -> build
        let actual = table("Item").select().project("*").offset(10);
        let expected = "SELECT * FROM Item OFFSET 10";
        test_query_builder(actual, expected);

        // inner hash join node -> offset node -> build
        let actual = table("Player")
            .select()
            .join("PlayerItem")
            .hash_executor("PlayerItem.user_id", "Player.id")
            .offset(100)
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
                input: ProjectInputPlan::InnerJoin(Box::new(join)),
                projection: ProjectionPlan::SelectItems(
                    SelectItemList::from("*").build_select_items_plan().unwrap(),
                ),
            };

            let offset = OffsetPlan {
                input: OffsetInputPlan::Project(project),
                count: num(100).build_expr_plan().unwrap(),
            };

            Ok(StatementPlan::Query(QueryPlan::Offset(offset)))
        };
        assert_eq!(actual, expected);

        // select -> offset -> derived subquery
        let actual = table("Foo").select().offset(10).alias_as("Sub").select();
        let expected = "SELECT * FROM (SELECT * FROM Foo OFFSET 10) Sub";
        test_query_builder(actual, expected);
    }
}
