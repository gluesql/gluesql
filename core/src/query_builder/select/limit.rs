use {
    super::{BuildQuery, BuildQueryBodyPlan, BuildQueryPlan, values::ValuesNode},
    crate::{
        ast::Query,
        plan::{LimitInputPlan, LimitPlan, QueryBodyPlan, QueryPlan},
        query_builder::{
            ExprNode, FilterNode, GroupByNode, HashJoinNode, HavingNode, JoinConstraintNode,
            JoinNode, OrderByNode, ProjectNode, QueryNode, SelectNode, TableFactorNode,
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
    OrderBy(OrderByNode<'a>),
    ProjectNode(Box<ProjectNode<'a>>),
}

impl BuildQueryBodyPlan for PrevNode<'_> {
    fn build_query_body_plan(self) -> Result<QueryBodyPlan> {
        match self {
            Self::Select(node) => node.build_query_body_plan(),
            Self::Values(node) => node.build_query_body_plan(),
            Self::GroupBy(node) => node.build_query_body_plan(),
            Self::Having(node) => node.build_query_body_plan(),
            Self::Join(node) => node.build_query_body_plan(),
            Self::JoinConstraint(node) => node.build_query_body_plan(),
            Self::HashJoin(node) => node.build_query_body_plan(),
            Self::Filter(node) => node.build_query_body_plan(),
            Self::OrderBy(node) => node.build_query_body_plan(),
            Self::ProjectNode(node) => node.build_query_body_plan(),
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
            Self::OrderBy(node) => node.build_query(),
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

impl<'a> From<OrderByNode<'a>> for PrevNode<'a> {
    fn from(node: OrderByNode<'a>) -> Self {
        PrevNode::OrderBy(node)
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

    pub fn alias_as(self, table_alias: &'a str) -> TableFactorNode<'a> {
        QueryNode::LimitNode(self).alias_as(table_alias)
    }
}

impl BuildQueryPlan for LimitNode<'_> {
    fn build_query_plan(self) -> Result<QueryPlan> {
        let count = self.expr.build_expr_plan()?;
        self.prev_node.build_query_body_plan().map(|body| {
            QueryPlan::Limit(LimitPlan {
                input: LimitInputPlan::Body(body),
                count,
            })
        })
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
                JoinConstraintPlan, JoinExecutorPlan, JoinOperatorPlan, JoinPlan, LimitInputPlan,
                LimitPlan, ProjectionPlan, QueryBodyPlan, QueryPlan, SelectPlan, SetExprPlan,
                StatementPlan, TableFactorPlan, TableWithJoinsPlan,
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

            let body = QueryBodyPlan {
                body: SetExprPlan::Select(Box::new(select)),
                order_by: Vec::new(),
            };
            let limit = LimitPlan {
                input: LimitInputPlan::Body(body),
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
