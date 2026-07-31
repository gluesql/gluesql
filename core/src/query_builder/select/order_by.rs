use {
    super::{BuildProjectPlan, BuildQuery, BuildQueryPlan, BuildSelect, DistinctNode, ValuesNode},
    crate::{
        ast::{OrderByExpr, Query, SetExpr},
        plan::{OrderByExprPlan, QueryPlan, SelectOrderByPlan, ValuesOrderByPlan},
        query_builder::{
            ExprNode, FilterNode, GroupByNode, HashJoinNode, HavingNode, JoinConstraintNode,
            JoinNode, LimitNode, OffsetNode, OrderByExprList, ProjectNode, QueryNode, SelectNode,
            TableFactorNode,
        },
        result::Result,
    },
};

#[derive(Clone, Debug)]
pub(super) enum SelectPrevNode<'a> {
    Select(SelectNode<'a>),
    Having(HavingNode<'a>),
    GroupBy(GroupByNode<'a>),
    Filter(FilterNode<'a>),
    JoinNode(JoinNode<'a>),
    JoinConstraint(JoinConstraintNode<'a>),
    HashJoin(Box<HashJoinNode<'a>>),
    ProjectNode(Box<ProjectNode<'a>>),
}

impl SelectPrevNode<'_> {
    fn build_select_order_by_plan(self, exprs: Vec<OrderByExprPlan>) -> Result<SelectOrderByPlan> {
        let input = match self {
            Self::Select(node) => node.build_project_plan(),
            Self::Having(node) => node.build_project_plan(),
            Self::GroupBy(node) => node.build_project_plan(),
            Self::Filter(node) => node.build_project_plan(),
            Self::JoinNode(node) => node.build_project_plan(),
            Self::JoinConstraint(node) => node.build_project_plan(),
            Self::HashJoin(node) => node.build_project_plan(),
            Self::ProjectNode(node) => node.build_project_plan(),
        };
        let input = input?;

        Ok(SelectOrderByPlan { input, exprs })
    }
}

impl<'a> From<SelectNode<'a>> for SelectPrevNode<'a> {
    fn from(node: SelectNode<'a>) -> Self {
        Self::Select(node)
    }
}

impl<'a> From<HavingNode<'a>> for SelectPrevNode<'a> {
    fn from(node: HavingNode<'a>) -> Self {
        Self::Having(node)
    }
}

impl<'a> From<GroupByNode<'a>> for SelectPrevNode<'a> {
    fn from(node: GroupByNode<'a>) -> Self {
        Self::GroupBy(node)
    }
}

impl<'a> From<FilterNode<'a>> for SelectPrevNode<'a> {
    fn from(node: FilterNode<'a>) -> Self {
        Self::Filter(node)
    }
}

impl<'a> From<JoinNode<'a>> for SelectPrevNode<'a> {
    fn from(node: JoinNode<'a>) -> Self {
        Self::JoinNode(node)
    }
}

impl<'a> From<JoinConstraintNode<'a>> for SelectPrevNode<'a> {
    fn from(node: JoinConstraintNode<'a>) -> Self {
        Self::JoinConstraint(node)
    }
}

impl<'a> From<HashJoinNode<'a>> for SelectPrevNode<'a> {
    fn from(node: HashJoinNode<'a>) -> Self {
        Self::HashJoin(Box::new(node))
    }
}

impl<'a> From<ProjectNode<'a>> for SelectPrevNode<'a> {
    fn from(node: ProjectNode<'a>) -> Self {
        Self::ProjectNode(Box::new(node))
    }
}

#[derive(Clone, Debug)]
pub struct SelectOrderByNode<'a> {
    prev_node: SelectPrevNode<'a>,
    expr_list: OrderByExprList<'a>,
}

impl<'a> SelectOrderByNode<'a> {
    pub(super) fn new<N: Into<SelectPrevNode<'a>>, T: Into<OrderByExprList<'a>>>(
        prev_node: N,
        expr_list: T,
    ) -> Self {
        Self {
            prev_node: prev_node.into(),
            expr_list: expr_list.into(),
        }
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

    pub fn alias_as(self, table_alias: &'a str) -> TableFactorNode<'a> {
        QueryNode::SelectOrderByNode(self).alias_as(table_alias)
    }
}

impl SelectOrderByNode<'_> {
    pub(super) fn build_select_order_by_plan(self) -> Result<SelectOrderByPlan> {
        let exprs = self.expr_list.build_order_by_exprs_plan()?;

        self.prev_node.build_select_order_by_plan(exprs)
    }

    pub(super) fn build_select_order_by(self) -> Result<(crate::ast::Select, Vec<OrderByExpr>)> {
        let select = match self.prev_node {
            SelectPrevNode::Select(node) => node.build_select(),
            SelectPrevNode::Having(node) => node.build_select(),
            SelectPrevNode::GroupBy(node) => node.build_select(),
            SelectPrevNode::Filter(node) => node.build_select(),
            SelectPrevNode::JoinNode(node) => node.build_select(),
            SelectPrevNode::JoinConstraint(node) => node.build_select(),
            SelectPrevNode::HashJoin(node) => node.build_select(),
            SelectPrevNode::ProjectNode(node) => node.build_select(),
        }?;
        let exprs = self.expr_list.build_order_by_exprs()?;

        Ok((select, exprs))
    }
}

impl BuildQueryPlan for SelectOrderByNode<'_> {
    fn build_query_plan(self) -> Result<QueryPlan> {
        self.build_select_order_by_plan()
            .map(QueryPlan::SelectOrderBy)
    }
}

impl BuildQuery for SelectOrderByNode<'_> {
    fn build_query(self) -> Result<Query> {
        let (select, order_by) = self.build_select_order_by()?;

        Ok(Query {
            body: SetExpr::Select(Box::new(select)),
            order_by,
            limit: None,
            offset: None,
        })
    }
}

#[derive(Clone, Debug)]
pub struct ValuesOrderByNode<'a> {
    prev_node: ValuesNode<'a>,
    expr_list: OrderByExprList<'a>,
}

impl<'a> ValuesOrderByNode<'a> {
    pub(super) fn new<T: Into<OrderByExprList<'a>>>(
        prev_node: ValuesNode<'a>,
        expr_list: T,
    ) -> Self {
        Self {
            prev_node,
            expr_list: expr_list.into(),
        }
    }

    pub fn offset<T: Into<ExprNode<'a>>>(self, expr: T) -> OffsetNode<'a> {
        OffsetNode::new(self, expr)
    }

    pub fn limit<T: Into<ExprNode<'a>>>(self, expr: T) -> LimitNode<'a> {
        LimitNode::new(self, expr)
    }

    pub fn alias_as(self, table_alias: &'a str) -> TableFactorNode<'a> {
        QueryNode::ValuesOrderByNode(self).alias_as(table_alias)
    }
}

impl ValuesOrderByNode<'_> {
    pub(super) fn build_values_order_by_plan(self) -> Result<ValuesOrderByPlan> {
        let input = self.prev_node.build_values_plan()?;
        let exprs = self.expr_list.build_order_by_exprs_plan()?;

        Ok(ValuesOrderByPlan { input, exprs })
    }
}

impl BuildQueryPlan for ValuesOrderByNode<'_> {
    fn build_query_plan(self) -> Result<QueryPlan> {
        self.build_values_order_by_plan()
            .map(QueryPlan::ValuesOrderBy)
    }
}

impl BuildQuery for ValuesOrderByNode<'_> {
    fn build_query(self) -> Result<Query> {
        let mut node_data = self.prev_node.build_query()?;
        node_data.order_by = self.expr_list.build_order_by_exprs()?;

        Ok(node_data)
    }
}

#[cfg(test)]
mod tests {
    use {
        crate::{
            plan::{
                JoinConstraintPlan, JoinExecutorPlan, JoinInputPlan, JoinOperatorPlan, JoinPlan,
                ProjectInputPlan, ProjectPlan, ProjectionPlan, QueryPlan, SelectOrderByPlan,
                StatementPlan, TableFactorPlan,
            },
            query_builder::{
                Build, ExprNode, OrderByExprList, SelectItemList, col, table, test_query_builder,
            },
        },
        pretty_assertions::assert_eq,
    };

    #[test]
    fn order_by() {
        // select node -> order by node(exprs vec) -> build
        let actual = table("Foo").select().order_by(vec!["name desc"]);
        let expected = "
            SELECT * FROM Foo
            ORDER BY name DESC
        ";
        test_query_builder(actual, expected);

        // select node -> order by node(exprs string) -> build
        let actual = table("Bar")
            .select()
            .order_by("name asc, id desc, country")
            .offset(10);
        let expected = "
                SELECT * FROM Bar
                ORDER BY name asc, id desc, country
                OFFSET 10
            ";
        test_query_builder(actual, expected);

        // group by node -> order by node -> build
        let actual = table("Bar")
            .select()
            .group_by("name")
            .order_by(vec!["id desc"]);
        let expected = "
                SELECT * FROM Bar
                GROUP BY name
                ORDER BY id desc
            ";
        test_query_builder(actual, expected);

        // having node -> order by node -> build
        let actual = table("Foo")
            .select()
            .group_by("city")
            .having("COUNT(name) < 100")
            .order_by(ExprNode::Identifier("name".into()))
            .offset(2)
            .limit(3);
        let expected = "
            SELECT * FROM Foo
            GROUP BY city
            HAVING COUNT(name) < 100
            ORDER BY name
            OFFSET 2
            LIMIT 3
        ";
        test_query_builder(actual, expected);

        // typed order by (single expression) -> build
        let actual = table("Item")
            .select()
            .project("name, price")
            .order_by(col("price").desc());
        let expected = "
            SELECT name, price FROM Item
            ORDER BY price DESC
        ";
        test_query_builder(actual, expected);

        // typed order by (multiple expressions) -> build
        let actual = table("Item")
            .select()
            .project("name, price")
            .order_by(vec![col("price").desc(), col("name").asc()]);
        let expected = "
            SELECT name, price FROM Item
            ORDER BY price DESC, name ASC
        ";
        test_query_builder(actual, expected);

        // filter node -> order by node -> build
        let actual = table("Foo")
            .select()
            .filter("id > 10")
            .filter("id < 20")
            .order_by("id asc");
        let expected = "
            SELECT * FROM Foo
            WHERE id > 10 AND id < 20
            ORDER BY id ASC";
        test_query_builder(actual, expected);

        // project node -> order by node -> build
        let actual = table("Foo").select().project("id").order_by("id asc");
        let expected = "SELECT id FROM Foo ORDER BY id asc";
        test_query_builder(actual, expected);

        // join node -> order by node -> build
        let actual = table("Foo").select().join("Bar").order_by("Foo.id desc");
        let expected = "
            SELECT * FROM Foo
            JOIN Bar
            ORDER BY Foo.id desc
        ";
        test_query_builder(actual, expected);

        // join constraint node -> order by node -> build
        let actual = table("Foo")
            .select()
            .join("Bar")
            .on("Foo.id = Bar.id")
            .order_by("Foo.id desc");
        let expected = "
            SELECT * FROM Foo
            JOIN Bar ON Foo.id = Bar.id
            ORDER BY Foo.id desc
        ";
        test_query_builder(actual, expected);

        // hash join node -> order by node -> build
        let actual = table("Player")
            .select()
            .join("PlayerItem")
            .hash_executor("PlayerItem.user_id", "Player.id")
            .order_by("Player.score DESC")
            .build();
        let expected = {
            let join = JoinPlan {
                input: JoinInputPlan::Relation(TableFactorPlan::Table {
                    name: "Player".to_owned(),
                    alias: None,
                    index: None,
                }),
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
            let project = ProjectPlan {
                input: ProjectInputPlan::Join(Box::new(join)),
                projection: ProjectionPlan::SelectItems(
                    SelectItemList::from("*").build_select_items_plan().unwrap(),
                ),
            };

            Ok(StatementPlan::Query(QueryPlan::SelectOrderBy(
                SelectOrderByPlan {
                    input: project,
                    exprs: OrderByExprList::from("Player.score DESC")
                        .build_order_by_exprs_plan()
                        .unwrap(),
                },
            )))
        };
        assert_eq!(actual, expected);

        // select -> order by node -> derived subquery
        let actual = table("Foo")
            .select()
            .order_by(vec!["name desc"])
            .alias_as("Sub")
            .select();
        let expected = "
            SELECT * FROM (
                SELECT * FROM Foo
                ORDER BY name DESC
            ) Sub
        ";
        test_query_builder(actual, expected);
    }
}
