use {
    super::{BuildProjectPlan, BuildQuery, BuildQueryPlan, DistinctNode, values::ValuesNode},
    crate::{
        ast::Query,
        plan::{LimitInputPlan, LimitPlan, QueryPlan},
        query_builder::{
            ExprNode, FilterNode, GroupByNode, HashJoinNode, HavingNode, JoinConstraintNode,
            JoinNode, ProjectNode, QueryNode, SelectNode, SelectOrderByNode, SourceNode,
            ValuesOrderByNode,
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
    fn build_limit_input_plan(self) -> Result<LimitInputPlan> {
        match self {
            Self::Select(node) => node.build_project_plan().map(LimitInputPlan::Project),
            Self::Values(node) => node.build_values_plan().map(LimitInputPlan::Values),
            Self::GroupBy(node) => node.build_project_plan().map(LimitInputPlan::Project),
            Self::Having(node) => node.build_project_plan().map(LimitInputPlan::Project),
            Self::Join(node) => node.build_project_plan().map(LimitInputPlan::Project),
            Self::JoinConstraint(node) => node.build_project_plan().map(LimitInputPlan::Project),
            Self::HashJoin(node) => node.build_project_plan().map(LimitInputPlan::Project),
            Self::Filter(node) => node.build_project_plan().map(LimitInputPlan::Project),
            Self::SelectOrderBy(node) => node
                .build_select_order_by_plan()
                .map(LimitInputPlan::SelectOrderBy),
            Self::ValuesOrderBy(node) => node
                .build_values_order_by_plan()
                .map(LimitInputPlan::ValuesOrderBy),
            Self::Distinct(node) => node.build_distinct_plan().map(LimitInputPlan::Distinct),
            Self::ProjectNode(node) => node.build_project_plan().map(LimitInputPlan::Project),
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
pub struct LimitNode<'a> {
    prev_node: PrevNode<'a>,
    expr: ExprNode<'a>,
}

impl<'a> LimitNode<'a> {
    pub(super) fn new<N: Into<PrevNode<'a>>, T: Into<ExprNode<'a>>>(prev_node: N, expr: T) -> Self {
        Self {
            prev_node: prev_node.into(),
            expr: expr.into(),
        }
    }

    pub fn alias_as(self, table_alias: &'a str) -> SourceNode<'a> {
        QueryNode::LimitNode(self).alias_as(table_alias)
    }
}

impl BuildQueryPlan for LimitNode<'_> {
    fn build_query_plan(self) -> Result<QueryPlan> {
        let count = self.expr.build_expr_plan()?;
        self.prev_node
            .build_limit_input_plan()
            .map(|input| QueryPlan::Limit(LimitPlan { input, count }))
    }
}

impl BuildQuery for LimitNode<'_> {
    fn build_query(self) -> Result<Query> {
        let mut node_data = self.prev_node.build_query()?;
        node_data.limit = Some(self.expr.build_expr()?);

        Ok(node_data)
    }
}

#[cfg(test)]
mod tests {
    use {
        crate::{
            plan::{
                JoinConstraintPlan, JoinExecutorPlan, JoinInputPlan, JoinOperatorPlan, JoinPlan,
                LimitInputPlan, LimitPlan, ProjectInputPlan, ProjectPlan, ProjectionPlan,
                QueryPlan, SourcePlan, StatementPlan, TableAccessPlan, TableSourcePlan,
            },
            query_builder::{Build, SelectItemList, col, num, table, test_query_builder},
        },
        pretty_assertions::assert_eq,
    };

    #[test]
    fn limit() {
        // select node -> limit node -> build
        let actual = table("Foo").select().limit(10);
        let expected = "SELECT * FROM Foo LIMIT 10";
        test_query_builder(actual, expected);

        // group by node -> limit node -> build
        let actual = table("Foo").select().group_by("bar").limit(10);
        let expected = "SELECT * FROM Foo GROUP BY bar LIMIT 10";
        test_query_builder(actual, expected);

        // having node -> limit node -> build
        let actual = table("Foo")
            .select()
            .group_by("bar")
            .having("bar = 10")
            .limit(10);
        let expected = "SELECT * FROM Foo GROUP BY bar HAVING bar = 10 LIMIT 10";
        test_query_builder(actual, expected);

        // join node -> limit node -> build
        let actual = table("Foo").select().join("Bar").limit(10);
        let expected = "SELECT * FROM Foo JOIN Bar LIMIT 10";
        test_query_builder(actual, expected);

        // join node -> limit node -> build
        let actual = table("Foo").select().join_as("Bar", "B").limit(10);
        let expected = "SELECT * FROM Foo JOIN Bar AS B LIMIT 10";
        test_query_builder(actual, expected);

        // join node -> limit node -> build
        let actual = table("Foo").select().left_join("Bar").limit(10);
        let expected = "SELECT * FROM Foo LEFT JOIN Bar LIMIT 10";
        test_query_builder(actual, expected);

        // join node -> limit node -> build
        let actual = table("Foo").select().left_join_as("Bar", "B").limit(10);
        let expected = "SELECT * FROM Foo LEFT JOIN Bar AS B LIMIT 10";
        test_query_builder(actual, expected);

        // group by node -> limit node -> build
        let actual = table("Foo").select().group_by("id").limit(10);
        let expected = "SELECT * FROM Foo GROUP BY id LIMIT 10";
        test_query_builder(actual, expected);

        // having node -> limit node -> build
        let actual = table("Foo")
            .select()
            .group_by("id")
            .having(col("id").gt(10))
            .limit(10);
        let expected = "SELECT * FROM Foo GROUP BY id HAVING id > 10 LIMIT 10";
        test_query_builder(actual, expected);

        // join constraint node -> limit node -> build
        let actual = table("Foo")
            .select()
            .join("Bar")
            .on("Foo.id = Bar.id")
            .limit(10);
        let expected = "SELECT * FROM Foo JOIN Bar ON Foo.id = Bar.id LIMIT 10";
        test_query_builder(actual, expected);

        // filter node -> limit node -> build
        let actual = table("World").select().filter(col("id").gt(2)).limit(100);
        let expected = "SELECT * FROM World WHERE id > 2 LIMIT 100";
        test_query_builder(actual, expected);

        // order by node -> limit node -> build
        let actual = table("Hello").select().order_by("score").limit(3);
        let expected = "SELECT * FROM Hello ORDER BY score LIMIT 3";
        test_query_builder(actual, expected);

        // project node -> limit node -> build
        let actual = table("Item").select().project("*").limit(10);
        let expected = "SELECT * FROM Item LIMIT 10";
        test_query_builder(actual, expected);

        // hash join node -> limit node -> build
        let actual = table("Player")
            .select()
            .join("PlayerItem")
            .hash_executor("PlayerItem.user_id", "Player.id")
            .limit(100)
            .build();
        let expected = {
            let join = JoinPlan {
                input: JoinInputPlan::Source(SourcePlan::Table(TableSourcePlan {
                    name: "Player".to_owned(),
                    alias: None,
                    access: TableAccessPlan::FullScan,
                })),
                right: SourcePlan::Table(TableSourcePlan {
                    name: "PlayerItem".to_owned(),
                    alias: None,
                    access: TableAccessPlan::FullScan,
                }),
                join_operator: JoinOperatorPlan::Inner(JoinConstraintPlan::None),
                join_executor: JoinExecutorPlan::Hash {
                    key_expr: col("PlayerItem.user_id").build_expr_plan().unwrap(),
                    value_expr: col("Player.id").build_expr_plan().unwrap(),
                    where_clause: None,
                },
            };
            let project = ProjectPlan {
                input: ProjectInputPlan::Join(Box::new(join)),
                projection: ProjectionPlan::SelectItems(
                    SelectItemList::from("*").build_select_items_plan().unwrap(),
                ),
            };

            let limit = LimitPlan {
                input: LimitInputPlan::Project(project),
                count: num(100).build_expr_plan().unwrap(),
            };

            Ok(StatementPlan::Query(QueryPlan::Limit(limit)))
        };
        assert_eq!(actual, expected);

        // select node -> limit node -> derived subquery
        let actual = table("Foo").select().limit(10).alias_as("Sub").select();
        let expected = "SELECT * FROM (SELECT * FROM Foo LIMIT 10) Sub";
        test_query_builder(actual, expected);
    }
}
