use {
    super::{NodeData, Prebuild},
    crate::{
        ast_builder::{
            ExprList, ExprNode, FilterNode, HashJoinNode, HavingNode, JoinConstraintNode, JoinNode,
            LimitNode, OffsetNode, OrderByExprList, OrderByNode, ProjectNode, SelectItemList,
            SelectNode,
        },
        result::Result,
    },
};

#[derive(Clone)]
pub enum PrevNode {
    Select(SelectNode),
    Join(Box<JoinNode>),
    JoinConstraint(Box<JoinConstraintNode>),
    HashJoin(Box<HashJoinNode>),
    Filter(FilterNode),
}

impl Prebuild for PrevNode {
    fn prebuild(self) -> Result<NodeData> {
        match self {
            Self::Select(node) => node.prebuild(),
            Self::Join(node) => node.prebuild(),
            Self::JoinConstraint(node) => node.prebuild(),
            Self::HashJoin(node) => node.prebuild(),
            Self::Filter(node) => node.prebuild(),
        }
    }
}

impl From<SelectNode> for PrevNode {
    fn from(node: SelectNode) -> Self {
        PrevNode::Select(node)
    }
}

impl From<JoinNode> for PrevNode {
    fn from(node: JoinNode) -> Self {
        PrevNode::Join(Box::new(node))
    }
}

impl From<JoinConstraintNode> for PrevNode {
    fn from(node: JoinConstraintNode) -> Self {
        PrevNode::JoinConstraint(Box::new(node))
    }
}

impl From<HashJoinNode> for PrevNode {
    fn from(node: HashJoinNode) -> Self {
        PrevNode::HashJoin(Box::new(node))
    }
}

impl From<FilterNode> for PrevNode {
    fn from(node: FilterNode) -> Self {
        PrevNode::Filter(node)
    }
}

#[derive(Clone)]
pub struct GroupByNode {
    prev_node: PrevNode,
    expr_list: ExprList,
}

impl GroupByNode {
    pub fn new<N: Into<PrevNode>, T: Into<ExprList>>(prev_node: N, expr_list: T) -> Self {
        Self {
            prev_node: prev_node.into(),
            expr_list: expr_list.into(),
        }
    }

    pub fn having<T: Into<ExprNode>>(self, expr: T) -> HavingNode {
        HavingNode::new(self, expr)
    }

    pub fn offset<T: Into<ExprNode>>(self, expr: T) -> OffsetNode {
        OffsetNode::new(self, expr)
    }

    pub fn limit<T: Into<ExprNode>>(self, expr: T) -> LimitNode {
        LimitNode::new(self, expr)
    }

    pub fn project<T: Into<SelectItemList>>(self, select_items: T) -> ProjectNode {
        ProjectNode::new(self, select_items)
    }

    pub fn order_by<T: Into<OrderByExprList>>(self, expr_list: T) -> OrderByNode {
        OrderByNode::new(self, expr_list)
    }
}

impl Prebuild for GroupByNode {
    fn prebuild(self) -> Result<NodeData> {
        let mut select_data = self.prev_node.prebuild()?;
        select_data.group_by = self.expr_list.try_into()?;

        Ok(select_data)
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        ast::{
            Join, JoinConstraint, JoinExecutor, JoinOperator, Query, Select, SetExpr, Statement,
            TableFactor, TableWithJoins,
        },
        ast_builder::{col, table, test, Build, SelectItemList},
    };

    #[test]
    fn group_by() {
        // select node -> group by node -> build
        let acutal = table("Foo").select().group_by("a").build();
        let expected = "SELECT * FROM Foo GROUP BY a";
        test(acutal, expected);

        // join node -> group by node -> build
        let actual = table("Foo").select().join("Bar").group_by("b").build();
        let expected = "SELECT * FROM Foo JOIN Bar GROUP BY b";
        test(actual, expected);

        // join node -> group by node -> build
        let actual = table("Foo")
            .select()
            .join_as("Bar", "B")
            .group_by("b")
            .build();
        let expected = "SELECT * FROM Foo JOIN Bar AS B GROUP BY b";
        test(actual, expected);

        // join node -> group by node -> build
        let actual = table("Foo").select().left_join("Bar").group_by("b").build();
        let expected = "SELECT * FROM Foo LEFT JOIN Bar GROUP BY b";
        test(actual, expected);

        // join node -> group by node -> build
        let actual = table("Foo")
            .select()
            .left_join_as("Bar", "B")
            .group_by("b")
            .build();
        let expected = "SELECT * FROM Foo LEFT JOIN Bar AS B GROUP BY b";
        test(actual, expected);

        // join constraint node -> group by node -> build
        let actual = table("Foo")
            .select()
            .join("Bar")
            .on("Foo.id = Bar.id")
            .group_by("b")
            .build();
        let expected = "SELECT * FROM Foo JOIN Bar ON Foo.id = Bar.id GROUP BY b";
        test(actual, expected);

        // filter node -> group by node -> build
        let actual = table("Bar")
            .select()
            .filter(col("id").is_null())
            .group_by("id, (a + name)")
            .build();
        let expected = "
                SELECT * FROM Bar
                WHERE id IS NULL
                GROUP BY id, (a + name)
            ";
        test(actual, expected);

        // hash join node -> group by node -> build
        let actual = table("Player")
            .select()
            .join("PlayerItem")
            .hash_executor("PlayerItem.user_id", "Player.id")
            .group_by("PlayerItem.category")
            .build();
        let expected = {
            let join = Join {
                relation: TableFactor::Table {
                    name: "PlayerItem".to_owned(),
                    alias: None,
                    index: None,
                },
                join_operator: JoinOperator::Inner(JoinConstraint::None),
                join_executor: JoinExecutor::Hash {
                    key_expr: col("PlayerItem.user_id").try_into().unwrap(),
                    value_expr: col("Player.id").try_into().unwrap(),
                    where_clause: None,
                },
            };
            let select = Select {
                projection: SelectItemList::from("*").try_into().unwrap(),
                from: TableWithJoins {
                    relation: TableFactor::Table {
                        name: "Player".to_owned(),
                        alias: None,
                        index: None,
                    },
                    joins: vec![join],
                },
                selection: None,
                group_by: vec![col("PlayerItem.category").try_into().unwrap()],
                having: None,
            };

            Ok(Statement::Query(Query {
                body: SetExpr::Select(Box::new(select)),
                order_by: Vec::new(),
                limit: None,
                offset: None,
            }))
        };
        assert_eq!(actual, expected);
    }
}
