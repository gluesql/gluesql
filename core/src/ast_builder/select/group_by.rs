use {
    super::Prebuild,
    crate::{
        ast::Select,
        ast_builder::{
            ExprList, ExprNode, FilterNode, HashJoinNode, HavingNode, JoinConstraintNode, JoinNode,
            LimitNode, OffsetNode, OrderByExprList, OrderByNode, ProjectNode, QueryNode,
            SelectItemList, SelectNode, TableFactorNode,
        },
        result::Result,
    },
};

#[derive(Clone, Debug)]
pub enum PrevNode<'a> {
    Select(SelectNode<'a>),
    Join(Box<JoinNode<'a>>),
    JoinConstraint(Box<JoinConstraintNode<'a>>),
    HashJoin(Box<HashJoinNode<'a>>),
    Filter(FilterNode<'a>),
}

impl<'a> Prebuild<Select> for PrevNode<'a> {
    fn prebuild(self) -> Result<Select> {
        match self {
            Self::Select(node) => node.prebuild(),
            Self::Join(node) => node.prebuild(),
            Self::JoinConstraint(node) => node.prebuild(),
            Self::HashJoin(node) => node.prebuild(),
            Self::Filter(node) => node.prebuild(),
        }
    }
}

impl<'a> From<SelectNode<'a>> for PrevNode<'a> {
    fn from(node: SelectNode<'a>) -> Self {
        PrevNode::Select(node)
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
pub struct GroupByNode<'a> {
    prev_node: PrevNode<'a>,
    expr_list: ExprList<'a>,
}

impl<'a> GroupByNode<'a> {
    pub fn new<N: Into<PrevNode<'a>>, T: Into<ExprList<'a>>>(prev_node: N, expr_list: T) -> Self {
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

    pub fn order_by<T: Into<OrderByExprList<'a>>>(self, expr_list: T) -> OrderByNode<'a> {
        OrderByNode::new(self, expr_list)
    }

    pub fn alias_as(self, table_alias: &'a str) -> TableFactorNode {
        QueryNode::GroupByNode(self).alias_as(table_alias)
    }
}

impl<'a> Prebuild<Select> for GroupByNode<'a> {
    fn prebuild(self) -> Result<Select> {
        let mut select: Select = self.prev_node.prebuild()?;
        select.group_by = self.expr_list.try_into()?;

        Ok(select)
    }
}

#[cfg(test)]
mod tests {
    use {
        crate::{
            ast::{
                Join, JoinConstraint, JoinExecutor, JoinOperator, Query, Select, SetExpr,
                Statement, TableFactor, TableWithJoins,
            },
            ast_builder::{col, table, test, Build, SelectItemList},
        },
        pretty_assertions::assert_eq,
    };

    #[test]
    fn group_by() {
        // select node -> group by node -> build
        let actual = table("Foo").select().group_by("a").build();
        let expected = "SELECT * FROM Foo GROUP BY a";
        test(actual, expected);

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

        // select -> group by -> derived subquery
        let actual = table("Foo")
            .select()
            .group_by("a")
            .alias_as("Sub")
            .select()
            .build();
        let expected = "SELECT * FROM (SELECT * FROM Foo GROUP BY a) Sub";
        test(actual, expected);
    }
}
