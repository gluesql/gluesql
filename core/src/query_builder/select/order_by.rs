use {
    super::{BuildQuery, BuildQueryPlan, ValuesNode},
    crate::{
        ast::Query,
        plan::QueryPlan,
        query_builder::{
            ExprNode, FilterNode, GroupByNode, HashJoinNode, HavingNode, JoinConstraintNode,
            JoinNode, LimitNode, OffsetNode, OrderByExprList, ProjectNode, QueryNode, SelectNode,
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
    JoinNode(JoinNode<'a>),
    JoinConstraint(JoinConstraintNode<'a>),
    HashJoin(Box<HashJoinNode<'a>>),
    ProjectNode(Box<ProjectNode<'a>>),
    Values(ValuesNode<'a>),
}

impl BuildQueryPlan for PrevNode<'_> {
    fn build_query_plan(self) -> Result<QueryPlan> {
        match self {
            Self::Select(node) => node.build_query_plan(),
            Self::Having(node) => node.build_query_plan(),
            Self::GroupBy(node) => node.build_query_plan(),
            Self::Filter(node) => node.build_query_plan(),
            Self::JoinNode(node) => node.build_query_plan(),
            Self::JoinConstraint(node) => node.build_query_plan(),
            Self::HashJoin(node) => node.build_query_plan(),
            Self::ProjectNode(node) => node.build_query_plan(),
            Self::Values(node) => node.build_query_plan(),
        }
    }
}

impl BuildQuery for PrevNode<'_> {
    fn build_query(self) -> Result<Query> {
        match self {
            Self::Select(node) => node.build_query(),
            Self::Having(node) => node.build_query(),
            Self::GroupBy(node) => node.build_query(),
            Self::Filter(node) => node.build_query(),
            Self::JoinNode(node) => node.build_query(),
            Self::JoinConstraint(node) => node.build_query(),
            Self::HashJoin(node) => node.build_query(),
            Self::ProjectNode(node) => node.build_query(),
            Self::Values(node) => node.build_query(),
        }
    }
}

impl<'a> From<SelectNode<'a>> for PrevNode<'a> {
    fn from(node: SelectNode<'a>) -> Self {
        PrevNode::Select(node)
    }
}

impl<'a> From<HavingNode<'a>> for PrevNode<'a> {
    fn from(node: HavingNode<'a>) -> Self {
        PrevNode::Having(node)
    }
}

impl<'a> From<GroupByNode<'a>> for PrevNode<'a> {
    fn from(node: GroupByNode<'a>) -> Self {
        PrevNode::GroupBy(node)
    }
}

impl<'a> From<FilterNode<'a>> for PrevNode<'a> {
    fn from(node: FilterNode<'a>) -> Self {
        PrevNode::Filter(node)
    }
}

impl<'a> From<JoinNode<'a>> for PrevNode<'a> {
    fn from(node: JoinNode<'a>) -> Self {
        PrevNode::JoinNode(node)
    }
}

impl<'a> From<JoinConstraintNode<'a>> for PrevNode<'a> {
    fn from(node: JoinConstraintNode<'a>) -> Self {
        PrevNode::JoinConstraint(node)
    }
}

impl<'a> From<HashJoinNode<'a>> for PrevNode<'a> {
    fn from(node: HashJoinNode<'a>) -> Self {
        PrevNode::HashJoin(Box::new(node))
    }
}

impl<'a> From<ProjectNode<'a>> for PrevNode<'a> {
    fn from(node: ProjectNode<'a>) -> Self {
        PrevNode::ProjectNode(Box::new(node))
    }
}

impl<'a> From<ValuesNode<'a>> for PrevNode<'a> {
    fn from(node: ValuesNode<'a>) -> Self {
        PrevNode::Values(node)
    }
}

#[derive(Clone, Debug)]
pub struct OrderByNode<'a> {
    prev_node: PrevNode<'a>,
    expr_list: OrderByExprList<'a>,
}

impl<'a> OrderByNode<'a> {
    pub(super) fn new<N: Into<PrevNode<'a>>, T: Into<OrderByExprList<'a>>>(
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

    pub fn alias_as(self, table_alias: &'a str) -> TableFactorNode<'a> {
        QueryNode::OrderByNode(self).alias_as(table_alias)
    }
}

impl BuildQueryPlan for OrderByNode<'_> {
    fn build_query_plan(self) -> Result<QueryPlan> {
        let mut node_data = self.prev_node.build_query_plan()?;
        node_data.order_by = self.expr_list.build_order_by_exprs_plan()?;

        Ok(node_data)
    }
}

impl BuildQuery for OrderByNode<'_> {
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
                JoinConstraintPlan, JoinExecutorPlan, JoinOperatorPlan, JoinPlan, ProjectionPlan,
                QueryPlan, SelectPlan, SetExprPlan, StatementPlan, TableFactorPlan,
                TableWithJoinsPlan,
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
                distinct: false,
                projection: ProjectionPlan::SelectItems(
                    SelectItemList::from("*").build_select_items_plan().unwrap(),
                ),
                from: TableWithJoinsPlan {
                    relation: TableFactorPlan::Table {
                        name: "Player".to_owned(),
                        alias: None,
                        index: None,
                    },
                    joins: vec![join],
                },
                selection: None,
                group_by: Vec::new(),
                having: None,
                aggregate_slots: None,
            };

            Ok(StatementPlan::Query(QueryPlan {
                body: SetExprPlan::Select(Box::new(select)),
                order_by: OrderByExprList::from("Player.score DESC")
                    .build_order_by_exprs_plan()
                    .unwrap(),
                limit: None,
                offset: None,
            }))
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
