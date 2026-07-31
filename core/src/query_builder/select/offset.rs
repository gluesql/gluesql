use {
    super::{BuildProjectPlan, BuildQuery, BuildQueryPlan, DistinctNode, ValuesNode},
    crate::{
        ast::Query,
        plan::{OffsetInputPlan, OffsetPlan, QueryPlan},
        query_builder::{
            ExprNode, FilterNode, GroupByNode, HashJoinNode, HavingNode, JoinConstraintNode,
            JoinNode, OffsetLimitNode, ProjectNode, QueryNode, SelectNode, SelectOrderByNode,
            TableFactorNode, ValuesOrderByNode,
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
    Join(Box<JoinNode<'a>>),
    JoinConstraint(Box<JoinConstraintNode<'a>>),
    HashJoin(HashJoinNode<'a>),
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
            Self::Join(node) => node.build_project_plan().map(OffsetInputPlan::Project),
            Self::JoinConstraint(node) => node.build_project_plan().map(OffsetInputPlan::Project),
            Self::HashJoin(node) => node.build_project_plan().map(OffsetInputPlan::Project),
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
            Self::Join(node) => node.build_query(),
            Self::JoinConstraint(node) => node.build_query(),
            Self::HashJoin(node) => node.build_query(),
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

impl<'a> From<JoinConstraintNode<'a>> for PrevNode<'a> {
    fn from(node: JoinConstraintNode<'a>) -> Self {
        PrevNode::JoinConstraint(Box::new(node))
    }
}

impl<'a> From<JoinNode<'a>> for PrevNode<'a> {
    fn from(node: JoinNode<'a>) -> Self {
        PrevNode::Join(Box::new(node))
    }
}

impl<'a> From<HashJoinNode<'a>> for PrevNode<'a> {
    fn from(node: HashJoinNode<'a>) -> Self {
        PrevNode::HashJoin(node)
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

    pub fn alias_as(self, table_alias: &'a str) -> TableFactorNode<'a> {
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
                JoinConstraintPlan, JoinExecutorPlan, JoinOperatorPlan, JoinPlan, OffsetInputPlan,
                OffsetPlan, ProjectInputPlan, ProjectPlan, ProjectionPlan, QueryPlan, SelectPlan,
                StatementPlan, TableFactorPlan, TableWithJoinsPlan,
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

        // join node -> offset node -> build
        let actual = table("Foo").select().join("Bar").offset(10);
        let expected = "SELECT * FROM Foo JOIN Bar OFFSET 10";
        test_query_builder(actual, expected);

        // join node -> offset node -> build
        let actual = table("Foo").select().join_as("Bar", "B").offset(10);
        let expected = "SELECT * FROM Foo JOIN Bar AS B OFFSET 10";
        test_query_builder(actual, expected);

        // join node -> offset node -> build
        let actual = table("Foo")
            .select()
            .left_join("Bar")
            .on("Foo.id = Bar.id")
            .offset(10);
        let expected = "SELECT * FROM Foo LEFT JOIN Bar ON Foo.id = Bar.id OFFSET 10";
        test_query_builder(actual, expected);

        // join node -> offset node -> build
        let actual = table("Foo")
            .select()
            .left_join_as("Bar", "B")
            .on("Foo.id = B.id")
            .offset(10);
        let expected = "SELECT * FROM Foo LEFT JOIN Bar AS B ON Foo.id = B.id OFFSET 10";
        test_query_builder(actual, expected);

        // join constraint node -> offset node -> build
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

        // hash join node -> offset node -> build
        let actual = table("Player")
            .select()
            .join("PlayerItem")
            .hash_executor("PlayerItem.user_id", "Player.id")
            .offset(100)
            .build();
        let expected = {
            let join = JoinPlan {
                relation: TableFactorPlan::Table {
                    name: "PlayerItem".to_owned(),
                    alias: None,
                    index: None,
                },
                join_operator: JoinOperatorPlan::Inner(JoinConstraintPlan::None),
                join_executor: JoinExecutorPlan::Hash {
                    key_expr: col("PlayerItem.user_id").build_expr_plan().unwrap(),
                    value_expr: col("Player.id").build_expr_plan().unwrap(),
                    where_clause: None,
                },
            };
            let select = SelectPlan {
                from: TableWithJoinsPlan {
                    relation: TableFactorPlan::Table {
                        name: "Player".to_owned(),
                        alias: None,
                        index: None,
                    },
                    joins: vec![join],
                },
                selection: None,
            };
            let project = ProjectPlan {
                input: ProjectInputPlan::Select(Box::new(select)),
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
