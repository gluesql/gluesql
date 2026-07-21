use {
    super::{BuildSelect, BuildSelectPlan},
    crate::{
        ast::Select,
        plan::SelectPlan,
        query_builder::{
            ExprList, ExprNode, FilterNode, HashJoinNode, HavingNode, JoinConstraintNode, JoinNode,
            LimitNode, OffsetNode, OrderByExprList, OrderByNode, ProjectNode, QueryNode,
            SelectItemList, SelectNode, TableFactorNode,
        },
        result::Result,
    },
};

#[derive(Clone, Debug)]
pub(super) enum PrevNode<'a> {
    Select(SelectNode<'a>),
    Join(Box<JoinNode<'a>>),
    JoinConstraint(Box<JoinConstraintNode<'a>>),
    HashJoin(Box<HashJoinNode<'a>>),
    Filter(FilterNode<'a>),
}

impl BuildSelectPlan for PrevNode<'_> {
    fn build_select_plan(self) -> Result<SelectPlan> {
        match self {
            Self::Select(node) => node.build_select_plan(),
            Self::Join(node) => node.build_select_plan(),
            Self::JoinConstraint(node) => node.build_select_plan(),
            Self::HashJoin(node) => node.build_select_plan(),
            Self::Filter(node) => node.build_select_plan(),
        }
    }
}

impl BuildSelect for PrevNode<'_> {
    fn build_select(self) -> Result<Select> {
        match self {
            Self::Select(node) => node.build_select(),
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
    pub(super) fn new<N: Into<PrevNode<'a>>, T: Into<ExprList<'a>>>(
        prev_node: N,
        expr_list: T,
    ) -> Self {
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

    pub fn alias_as(self, table_alias: &'a str) -> TableFactorNode<'a> {
        QueryNode::GroupByNode(self).alias_as(table_alias)
    }
}

impl BuildSelectPlan for GroupByNode<'_> {
    fn build_select_plan(self) -> Result<SelectPlan> {
        let mut select = self.prev_node.build_select_plan()?;
        select.group_by = self.expr_list.build_exprs_plan()?;

        Ok(select)
    }
}

impl BuildSelect for GroupByNode<'_> {
    fn build_select(self) -> Result<Select> {
        let mut select = self.prev_node.build_select()?;
        select.group_by = self.expr_list.build_exprs()?;

        Ok(select)
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
            query_builder::{Build, SelectItemList, col, table, test_query_builder},
        },
        pretty_assertions::assert_eq,
    };

    #[test]
    fn group_by() {
        // select node -> group by node -> build
        let actual = table("Foo").select().group_by("a");
        let expected = "SELECT * FROM Foo GROUP BY a";
        test_query_builder(actual, expected);

        // join node -> group by node -> build
        let actual = table("Foo").select().join("Bar").group_by("b");
        let expected = "SELECT * FROM Foo JOIN Bar GROUP BY b";
        test_query_builder(actual, expected);

        // join node -> group by node -> build
        let actual = table("Foo").select().join_as("Bar", "B").group_by("b");
        let expected = "SELECT * FROM Foo JOIN Bar AS B GROUP BY b";
        test_query_builder(actual, expected);

        // join node -> group by node -> build
        let actual = table("Foo").select().left_join("Bar").group_by("b");
        let expected = "SELECT * FROM Foo LEFT JOIN Bar GROUP BY b";
        test_query_builder(actual, expected);

        // join node -> group by node -> build
        let actual = table("Foo").select().left_join_as("Bar", "B").group_by("b");
        let expected = "SELECT * FROM Foo LEFT JOIN Bar AS B GROUP BY b";
        test_query_builder(actual, expected);

        // join constraint node -> group by node -> build
        let actual = table("Foo")
            .select()
            .join("Bar")
            .on("Foo.id = Bar.id")
            .group_by("b");
        let expected = "SELECT * FROM Foo JOIN Bar ON Foo.id = Bar.id GROUP BY b";
        test_query_builder(actual, expected);

        // filter node -> group by node -> build
        let actual = table("Bar")
            .select()
            .filter(col("id").is_null())
            .group_by("id, (a + name)");
        let expected = "
                SELECT * FROM Bar
                WHERE id IS NULL
                GROUP BY id, (a + name)
            ";
        test_query_builder(actual, expected);

        // hash join node -> group by node -> build
        let actual = table("Player")
            .select()
            .join("PlayerItem")
            .hash_executor("PlayerItem.user_id", "Player.id")
            .group_by("PlayerItem.category")
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
                group_by: vec![col("PlayerItem.category").build_expr_plan().unwrap()],
                having: None,
                aggregate_slots: None,
            };

            Ok(StatementPlan::Query(QueryPlan::Body(SetExprPlan::Select(
                Box::new(select),
            ))))
        };
        assert_eq!(actual, expected);

        // select -> group by -> derived subquery
        let actual = table("Foo").select().group_by("a").alias_as("Sub").select();
        let expected = "SELECT * FROM (SELECT * FROM Foo GROUP BY a) Sub";
        test_query_builder(actual, expected);
    }
}
