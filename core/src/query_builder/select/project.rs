use {
    super::{BuildProjectInputPlan, BuildProjectPlan, BuildSelect, DistinctNode},
    crate::{
        ast::{Projection, Select},
        plan::{ProjectInputPlan, ProjectPlan, ProjectionPlan},
        query_builder::{
            ExprNode, FilterNode, GroupByNode, HashJoinNode, HavingNode, JoinConstraintNode,
            JoinNode, LimitNode, OffsetNode, OrderByExprList, QueryNode, SelectItemList,
            SelectNode, SelectOrderByNode, SourceNode,
        },
        result::Result,
    },
};

#[derive(Clone, Debug)]
pub(super) enum PrevNode<'a> {
    Select(SelectNode<'a>),
    GroupBy(GroupByNode<'a>),
    Having(HavingNode<'a>),
    Join(Box<JoinNode<'a>>),
    JoinConstraint(Box<JoinConstraintNode<'a>>),
    HashJoin(Box<HashJoinNode<'a>>),
    Filter(FilterNode<'a>),
}

impl BuildProjectInputPlan for PrevNode<'_> {
    fn build_project_input_plan(self) -> Result<ProjectInputPlan> {
        match self {
            Self::Select(node) => node.build_project_input_plan(),
            Self::GroupBy(node) => node.build_project_input_plan(),
            Self::Having(node) => node.build_project_input_plan(),
            Self::Join(node) => node.build_project_input_plan(),
            Self::JoinConstraint(node) => node.build_project_input_plan(),
            Self::HashJoin(node) => node.build_project_input_plan(),
            Self::Filter(node) => node.build_project_input_plan(),
        }
    }
}

impl BuildSelect for PrevNode<'_> {
    fn build_select(self) -> Result<Select> {
        match self {
            Self::Select(node) => node.build_select(),
            Self::GroupBy(node) => node.build_select(),
            Self::Having(node) => node.build_select(),
            Self::Join(node) => node.build_select(),
            Self::JoinConstraint(node) => node.build_select(),
            Self::HashJoin(node) => node.build_select(),
            Self::Filter(node) => node.build_select(),
        }
    }
}

impl<'a> From<SelectNode<'a>> for PrevNode<'a> {
    fn from(node: SelectNode<'a>) -> Self {
        PrevNode::Select(node)
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

impl<'a> From<JoinNode<'a>> for PrevNode<'a> {
    fn from(node: JoinNode<'a>) -> Self {
        PrevNode::Join(Box::new(node))
    }
}

impl<'a> From<JoinConstraintNode<'a>> for PrevNode<'a> {
    fn from(node: JoinConstraintNode<'a>) -> Self {
        PrevNode::JoinConstraint(Box::new(node))
    }
}

impl<'a> From<HashJoinNode<'a>> for PrevNode<'a> {
    fn from(node: HashJoinNode<'a>) -> Self {
        PrevNode::HashJoin(Box::new(node))
    }
}

impl<'a> From<FilterNode<'a>> for PrevNode<'a> {
    fn from(node: FilterNode<'a>) -> Self {
        PrevNode::Filter(node)
    }
}

#[derive(Clone, Debug)]
pub struct ProjectNode<'a> {
    prev_node: PrevNode<'a>,
    select_items_list: Vec<SelectItemList<'a>>,
}

impl<'a> ProjectNode<'a> {
    pub(super) fn new<N: Into<PrevNode<'a>>, T: Into<SelectItemList<'a>>>(
        prev_node: N,
        select_items: T,
    ) -> Self {
        Self {
            prev_node: prev_node.into(),
            select_items_list: vec![select_items.into()],
        }
    }

    #[must_use]
    pub fn project<T: Into<SelectItemList<'a>>>(mut self, select_items: T) -> Self {
        self.select_items_list.push(select_items.into());

        self
    }

    pub fn alias_as(self, table_alias: &'a str) -> SourceNode<'a> {
        QueryNode::ProjectNode(self).alias_as(table_alias)
    }

    pub fn order_by<T: Into<OrderByExprList<'a>>>(
        self,
        order_by_exprs: T,
    ) -> SelectOrderByNode<'a> {
        SelectOrderByNode::new(self, order_by_exprs)
    }

    pub fn offset<T: Into<ExprNode<'a>>>(self, expr: T) -> OffsetNode<'a> {
        OffsetNode::new(self, expr)
    }

    pub fn limit<T: Into<ExprNode<'a>>>(self, expr: T) -> LimitNode<'a> {
        LimitNode::new(self, expr)
    }

    pub fn distinct(self) -> DistinctNode<'a> {
        DistinctNode::new(self)
    }
}

impl BuildProjectPlan for ProjectNode<'_> {
    fn build_project_plan(self) -> Result<ProjectPlan> {
        let input = self.prev_node.build_project_input_plan()?;
        let projection = ProjectionPlan::SelectItems(
            self.select_items_list
                .into_iter()
                .map(SelectItemList::build_select_items_plan)
                .collect::<Result<Vec<Vec<_>>>>()?
                .into_iter()
                .flatten()
                .collect::<Vec<_>>(),
        );

        Ok(ProjectPlan { input, projection })
    }
}

impl BuildSelect for ProjectNode<'_> {
    fn build_select(self) -> Result<Select> {
        let mut query = self.prev_node.build_select()?;
        query.projection = Projection::SelectItems(
            self.select_items_list
                .into_iter()
                .map(SelectItemList::build_select_items)
                .collect::<Result<Vec<Vec<_>>>>()?
                .into_iter()
                .flatten()
                .collect::<Vec<_>>(),
        );

        Ok(query)
    }
}

#[cfg(test)]
mod tests {
    use {
        crate::{
            plan::{
                JoinConstraintPlan, JoinExecutorPlan, JoinInputPlan, JoinOperatorPlan, JoinPlan,
                ProjectInputPlan, ProjectPlan, ProjectionPlan, QueryPlan, SourcePlan,
                StatementPlan, TableAccessPlan, TableSourcePlan,
            },
            query_builder::{Build, SelectItemList, col, table, test_query_builder},
        },
        pretty_assertions::assert_eq,
    };

    #[test]
    fn project() {
        // select node -> project node -> build
        let actual = table("Good").select().project("id");
        let expected = "SELECT id FROM Good";
        test_query_builder(actual, expected);

        // select node -> project node -> build
        let actual = table("Group").select().project("*, Group.*, name");
        let expected = "SELECT *, Group.*, name FROM Group";
        test_query_builder(actual, expected);

        // project node -> project node -> build
        let actual = table("Foo")
            .select()
            .project(vec!["col1", "col2"])
            .project("col3")
            .project(vec!["col4".into(), col("col5")])
            .project(col("col6"))
            .project("col7 as hello");
        let expected = "
            SELECT
                col1, col2, col3,
                col4, col5, col6,
                col7 as hello
            FROM
                Foo
        ";
        test_query_builder(actual, expected);

        // select node -> project node -> build
        let actual = table("Aliased").select().project("1 + 1 as col1, col2");
        let expected = "SELECT 1 + 1 as col1, col2 FROM Aliased";
        test_query_builder(actual, expected);
    }

    #[test]
    fn prev_nodes() {
        // select node -> project node -> build
        let actual = table("Foo").select().project("*");
        let expected = "SELECT * FROM Foo";
        test_query_builder(actual, expected);

        // group by node -> project node -> build
        let actual = table("Bar")
            .select()
            .group_by("city")
            .project("city, COUNT(name) as num");
        let expected = "
            SELECT
              city, COUNT(name) as num
            FROM Bar
            GROUP BY city
        ";
        test_query_builder(actual, expected);

        // having node -> project node -> build
        let actual = table("Cat")
            .select()
            .filter(r#"type = "cute""#)
            .group_by("age")
            .having("SUM(length) < 1000")
            .project(col("age"))
            .project("SUM(length)");
        let expected = r#"
            SELECT age, SUM(length)
            FROM Cat
            WHERE type = "cute"
            GROUP BY age
            HAVING SUM(length) < 1000;
        "#;
        test_query_builder(actual, expected);

        // filter node -> project node -> build
        let actual = table("Item").select().filter("price > 10").project("name");
        let expected = "SELECT name FROM Item WHERE price > 10";
        test_query_builder(actual, expected);

        // join node -> project node -> build
        let actual = table("Item").select().join("Category").project("Item.name");
        let expected = "SELECT Item.name FROM Item JOIN Category";
        test_query_builder(actual, expected);

        // join constraint node -> project node -> build
        let actual = table("Item")
            .select()
            .join("Category")
            .on("Item.category_id = Category.id")
            .project("Item.name");
        let expected = "SELECT Item.name FROM Item JOIN Category ON Item.category_id = Category.id";
        test_query_builder(actual, expected);

        // hash join node -> project node -> build
        let actual = table("Player")
            .select()
            .join("PlayerItem")
            .hash_executor("PlayerItem.user_id", "Player.id")
            .project("Player.name, PlayerItem.name")
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
                    SelectItemList::from("Player.name, PlayerItem.name")
                        .build_select_items_plan()
                        .unwrap(),
                ),
            };

            Ok(StatementPlan::Query(QueryPlan::Project(project)))
        };
        assert_eq!(actual, expected);

        // select -> project -> derived subquery
        let actual = table("Foo").select().project("id").alias_as("Sub").select();
        let expected = "SELECT * FROM (SELECT id FROM Foo) Sub";
        test_query_builder(actual, expected);
    }
}
