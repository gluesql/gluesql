use {
    super::{BuildProjectPlan, BuildQuery, BuildQueryPlan, BuildSelect},
    crate::{
        ast::{OrderByExpr, Query, SetExpr},
        plan::{DistinctInputPlan, DistinctPlan, QueryPlan},
        query_builder::{
            ExprNode, FilterNode, GroupByNode, HashJoinNode, HavingNode, JoinConstraintNode,
            JoinNode, LimitNode, OffsetNode, ProjectNode, QueryNode, SelectNode, SelectOrderByNode,
            TableFactorNode,
        },
        result::Result,
    },
};

#[derive(Clone, Debug)]
pub(super) enum PrevNode<'a> {
    Select(SelectNode<'a>),
    Having(HavingNode<'a>),
    GroupBy(GroupByNode<'a>),
    Filter(FilterNode<'a>),
    Join(JoinNode<'a>),
    JoinConstraint(JoinConstraintNode<'a>),
    HashJoin(Box<HashJoinNode<'a>>),
    Project(Box<ProjectNode<'a>>),
    SelectOrderBy(SelectOrderByNode<'a>),
}

impl PrevNode<'_> {
    fn build_distinct_input_plan(self) -> Result<DistinctInputPlan> {
        match self {
            Self::Select(node) => node.build_project_plan().map(DistinctInputPlan::Project),
            Self::Having(node) => node.build_project_plan().map(DistinctInputPlan::Project),
            Self::GroupBy(node) => node.build_project_plan().map(DistinctInputPlan::Project),
            Self::Filter(node) => node.build_project_plan().map(DistinctInputPlan::Project),
            Self::Join(node) => node.build_project_plan().map(DistinctInputPlan::Project),
            Self::JoinConstraint(node) => node.build_project_plan().map(DistinctInputPlan::Project),
            Self::HashJoin(node) => node.build_project_plan().map(DistinctInputPlan::Project),
            Self::Project(node) => node.build_project_plan().map(DistinctInputPlan::Project),
            Self::SelectOrderBy(node) => node
                .build_select_order_by_plan()
                .map(DistinctInputPlan::SelectOrderBy),
        }
    }

    fn build_select(self) -> Result<(crate::ast::Select, Vec<OrderByExpr>)> {
        let select = match self {
            Self::Select(node) => node.build_select(),
            Self::Having(node) => node.build_select(),
            Self::GroupBy(node) => node.build_select(),
            Self::Filter(node) => node.build_select(),
            Self::Join(node) => node.build_select(),
            Self::JoinConstraint(node) => node.build_select(),
            Self::HashJoin(node) => node.build_select(),
            Self::Project(node) => node.build_select(),
            Self::SelectOrderBy(node) => return node.build_select_order_by(),
        }?;

        Ok((select, Vec::new()))
    }
}

macro_rules! impl_from_select_node {
    ($type: ident, $variant: ident) => {
        impl<'a> From<$type<'a>> for PrevNode<'a> {
            fn from(node: $type<'a>) -> Self {
                Self::$variant(node)
            }
        }
    };
}

impl_from_select_node!(SelectNode, Select);
impl_from_select_node!(HavingNode, Having);
impl_from_select_node!(GroupByNode, GroupBy);
impl_from_select_node!(FilterNode, Filter);
impl_from_select_node!(JoinNode, Join);
impl_from_select_node!(JoinConstraintNode, JoinConstraint);
impl_from_select_node!(SelectOrderByNode, SelectOrderBy);

impl<'a> From<HashJoinNode<'a>> for PrevNode<'a> {
    fn from(node: HashJoinNode<'a>) -> Self {
        Self::HashJoin(Box::new(node))
    }
}

impl<'a> From<ProjectNode<'a>> for PrevNode<'a> {
    fn from(node: ProjectNode<'a>) -> Self {
        Self::Project(Box::new(node))
    }
}

#[derive(Clone, Debug)]
pub struct DistinctNode<'a> {
    prev_node: PrevNode<'a>,
}

impl<'a> DistinctNode<'a> {
    pub(super) fn new<N: Into<PrevNode<'a>>>(prev_node: N) -> Self {
        Self {
            prev_node: prev_node.into(),
        }
    }

    pub fn offset<T: Into<ExprNode<'a>>>(self, expr: T) -> OffsetNode<'a> {
        OffsetNode::new(self, expr)
    }

    pub fn limit<T: Into<ExprNode<'a>>>(self, expr: T) -> LimitNode<'a> {
        LimitNode::new(self, expr)
    }

    pub fn alias_as(self, table_alias: &'a str) -> TableFactorNode<'a> {
        QueryNode::DistinctNode(self).alias_as(table_alias)
    }

    pub(super) fn build_distinct_plan(self) -> Result<DistinctPlan> {
        self.prev_node
            .build_distinct_input_plan()
            .map(|input| DistinctPlan { input })
    }
}

impl BuildQueryPlan for DistinctNode<'_> {
    fn build_query_plan(self) -> Result<QueryPlan> {
        self.build_distinct_plan().map(QueryPlan::Distinct)
    }
}

impl BuildQuery for DistinctNode<'_> {
    fn build_query(self) -> Result<Query> {
        let (mut select, order_by) = self.prev_node.build_select()?;
        select.distinct = true;

        Ok(Query {
            body: SetExpr::Select(Box::new(select)),
            order_by,
            limit: None,
            offset: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        plan::{
            DistinctInputPlan, DistinctPlan, JoinConstraintPlan, JoinExecutorPlan,
            JoinOperatorPlan, JoinPlan, ProjectInputPlan, ProjectPlan, ProjectionPlan, QueryPlan,
            SelectPlan, StatementPlan, TableFactorPlan, TableWithJoinsPlan,
        },
        query_builder::{
            Build, QueryBuilderError, SelectItemList, col, select::BuildQuery, table,
            test_query_builder,
        },
        result::Error,
    };
    use pretty_assertions::assert_eq;

    #[test]
    fn distinct_accepts_select_stage_inputs() {
        let actual = table("Item").select().distinct();
        let expected = "SELECT DISTINCT * FROM Item";
        test_query_builder(actual, expected);

        let actual = table("Item").select().filter("id > 0").distinct();
        let expected = "SELECT DISTINCT * FROM Item WHERE id > 0";
        test_query_builder(actual, expected);

        let actual = table("Item").select().group_by("name").distinct();
        let expected = "SELECT DISTINCT * FROM Item GROUP BY name";
        test_query_builder(actual, expected);

        let actual = table("Item")
            .select()
            .group_by("name")
            .having("COUNT(*) > 0")
            .distinct();
        let expected = "SELECT DISTINCT * FROM Item GROUP BY name HAVING COUNT(*) > 0";
        test_query_builder(actual, expected);

        let actual = table("Item").select().join("Category").distinct();
        let expected = "SELECT DISTINCT * FROM Item JOIN Category";
        test_query_builder(actual, expected);

        let actual = table("Item")
            .select()
            .join("Category")
            .on("Item.category_id = Category.id")
            .distinct();
        let expected =
            "SELECT DISTINCT * FROM Item JOIN Category ON Item.category_id = Category.id";
        test_query_builder(actual, expected);

        let actual = table("Item").select().project("name").distinct();
        let expected = "SELECT DISTINCT name FROM Item";
        test_query_builder(actual, expected);

        let actual = table("Item")
            .select()
            .project("name")
            .order_by("name")
            .distinct();
        let expected = "SELECT DISTINCT name FROM Item ORDER BY name";
        test_query_builder(actual, expected);
    }

    #[test]
    fn distinct_connects_to_terminal_stages() {
        let actual = table("Item").select().distinct().offset(1);
        let expected = "SELECT DISTINCT * FROM Item OFFSET 1";
        test_query_builder(actual, expected);

        let actual = table("Item").select().distinct().limit(1);
        let expected = "SELECT DISTINCT * FROM Item LIMIT 1";
        test_query_builder(actual, expected);

        let actual = table("Item")
            .select()
            .distinct()
            .alias_as("DistinctItem")
            .select();
        let expected = "SELECT * FROM (SELECT DISTINCT * FROM Item) DistinctItem";
        test_query_builder(actual, expected);
    }

    #[test]
    fn distinct_preserves_plan_only_hash_join() {
        let node = table("Item")
            .select()
            .join("Category")
            .hash_executor("Item.category_id", "Category.id")
            .distinct();

        assert_eq!(
            node.clone().build_query(),
            Err(Error::QueryBuilder(
                QueryBuilderError::HashJoinExecutorRequiresPlan,
            ))
        );

        let actual = node.build();
        let expected = {
            let join = JoinPlan {
                relation: TableFactorPlan::Table {
                    name: "Category".to_owned(),
                    alias: None,
                    index: None,
                },
                join_operator: JoinOperatorPlan::Inner(JoinConstraintPlan::None),
                join_executor: JoinExecutorPlan::Hash {
                    key_expr: col("Item.category_id").build_expr_plan().unwrap(),
                    value_expr: col("Category.id").build_expr_plan().unwrap(),
                    where_clause: None,
                },
            };
            let select = SelectPlan {
                from: TableWithJoinsPlan {
                    relation: TableFactorPlan::Table {
                        name: "Item".to_owned(),
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

            Ok(StatementPlan::Query(QueryPlan::Distinct(DistinctPlan {
                input: DistinctInputPlan::Project(project),
            })))
        };
        assert_eq!(actual, expected);
    }
}
