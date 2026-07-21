use {
    super::JoinOperatorType,
    crate::{
        ast::{Expr, Select},
        plan::{JoinConstraintPlan, SelectPlan},
        query_builder::{
            ExprList, ExprNode, FilterNode, GroupByNode, HashJoinNode, JoinNode, LimitNode,
            OffsetNode, OrderByExprList, OrderByNode, ProjectNode, QueryBuilderError, QueryNode,
            SelectItemList, TableFactorNode,
            select::{BuildSelect, BuildSelectPlan},
        },
        result::Result,
    },
};

#[derive(Clone, Debug)]
pub(super) enum PrevNode<'a> {
    Join(Box<JoinNode<'a>>),
    HashJoin(Box<HashJoinNode<'a>>),
}

impl PrevNode<'_> {
    fn build_select_plan_with_constraint(
        self,
        constraint: JoinConstraintPlan,
    ) -> Result<SelectPlan> {
        match self {
            PrevNode::Join(node) => node.build_select_plan_with_constraint(constraint),
            PrevNode::HashJoin(node) => node.build_select_plan_with_constraint(constraint),
        }
    }

    fn build_select_with_constraint(self, constraint: Expr) -> Result<Select> {
        match self {
            PrevNode::Join(node) => node.build_select_with_constraint(constraint),
            PrevNode::HashJoin(_) => Err(QueryBuilderError::HashJoinExecutorRequiresPlan.into()),
        }
    }
}

impl<'a> From<JoinNode<'a>> for PrevNode<'a> {
    fn from(node: JoinNode<'a>) -> Self {
        PrevNode::Join(Box::new(node))
    }
}

impl<'a> From<HashJoinNode<'a>> for PrevNode<'a> {
    fn from(node: HashJoinNode<'a>) -> Self {
        PrevNode::HashJoin(Box::new(node))
    }
}

#[derive(Clone, Debug)]
pub struct JoinConstraintNode<'a> {
    prev_node: PrevNode<'a>,
    expr: ExprNode<'a>,
}

impl<'a> JoinConstraintNode<'a> {
    pub(super) fn new<N: Into<PrevNode<'a>>, T: Into<ExprNode<'a>>>(prev_node: N, expr: T) -> Self {
        Self {
            prev_node: prev_node.into(),
            expr: expr.into(),
        }
    }

    pub fn join(self, table_name: &str) -> JoinNode<'a> {
        JoinNode::new(self, table_name.to_owned(), None, JoinOperatorType::Inner)
    }

    pub fn join_as(self, table_name: &str, alias: &str) -> JoinNode<'a> {
        JoinNode::new(
            self,
            table_name.to_owned(),
            Some(alias.to_owned()),
            JoinOperatorType::Inner,
        )
    }

    pub fn left_join(self, table_name: &str) -> JoinNode<'a> {
        JoinNode::new(self, table_name.to_owned(), None, JoinOperatorType::Left)
    }

    pub fn left_join_as(self, table_name: &str, alias: &str) -> JoinNode<'a> {
        JoinNode::new(
            self,
            table_name.to_owned(),
            Some(alias.to_owned()),
            JoinOperatorType::Left,
        )
    }

    pub fn project<T: Into<SelectItemList<'a>>>(self, select_items: T) -> ProjectNode<'a> {
        ProjectNode::new(self, select_items)
    }

    pub fn group_by<T: Into<ExprList<'a>>>(self, expr_list: T) -> GroupByNode<'a> {
        GroupByNode::new(self, expr_list)
    }

    pub fn offset<T: Into<ExprNode<'a>>>(self, expr: T) -> OffsetNode<'a> {
        OffsetNode::new(self, expr)
    }

    pub fn limit<T: Into<ExprNode<'a>>>(self, expr: T) -> LimitNode<'a> {
        LimitNode::new(self, expr)
    }

    pub fn filter<T: Into<ExprNode<'a>>>(self, expr: T) -> FilterNode<'a> {
        FilterNode::new(self, expr)
    }

    pub fn order_by<T: Into<OrderByExprList<'a>>>(self, order_by_exprs: T) -> OrderByNode<'a> {
        OrderByNode::new(self, order_by_exprs)
    }

    pub fn alias_as(self, table_alias: &'a str) -> TableFactorNode<'a> {
        QueryNode::JoinConstraintNode(self).alias_as(table_alias)
    }
}

impl BuildSelectPlan for JoinConstraintNode<'_> {
    fn build_select_plan(self) -> Result<SelectPlan> {
        let constraint = JoinConstraintPlan::On(self.expr.build_expr_plan()?);

        self.prev_node.build_select_plan_with_constraint(constraint)
    }
}

impl BuildSelect for JoinConstraintNode<'_> {
    fn build_select(self) -> Result<Select> {
        let constraint = self.expr.build_expr()?;

        self.prev_node.build_select_with_constraint(constraint)
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
    fn join_constraint() {
        // join node ->  join constarint node -> build
        let actual = table("Foo").select().join("Bar").on("Foo.id = Bar.id");
        let expected = "SELECT * FROM Foo INNER JOIN Bar ON Foo.id = Bar.id";
        test_query_builder(actual, expected);

        // join node ->  join constraint node -> build
        let actual = table("Foo")
            .select()
            .join_as("Bar", "B")
            .on("Foo.id = B.id");
        let expected = "SELECT * FROM Foo INNER JOIN Bar B ON Foo.id = B.id";
        test_query_builder(actual, expected);

        // join node -> join constraint node -> build
        let actual = table("Foo").select().left_join("Bar").on("Foo.id = Bar.id");
        let expected = "SELECT * FROM Foo LEFT OUTER JOIN Bar ON Foo.id = Bar.id";
        test_query_builder(actual, expected);

        // join node -> join constraint node -> build
        let actual = table("Foo")
            .select()
            .left_join_as("Bar", "b")
            .on("Foo.id = b.id");
        let expected = "SELECT * FROM Foo LEFT OUTER JOIN Bar b ON Foo.id = b.id";
        test_query_builder(actual, expected);

        // hash join node -> join constraint node -> build
        let actual = table("Player")
            .select()
            .join("PlayerItem")
            .hash_executor("PlayerItem.user_id", "Player.id")
            .on("PlayerItem.flag IS NOT NULL")
            .build();
        let expected = {
            let join = JoinPlan {
                relation: TableFactorPlan::Table {
                    name: "PlayerItem".to_owned(),
                    alias: None,
                    index: None,
                },
                join_operator: JoinOperatorPlan::Inner(JoinConstraintPlan::On(
                    col("PlayerItem.flag")
                        .is_not_null()
                        .build_expr_plan()
                        .unwrap(),
                )),
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

            Ok(StatementPlan::Query(QueryPlan::Body(SetExprPlan::Select(
                Box::new(select),
            ))))
        };
        assert_eq!(actual, expected, "hash join -> join constraint");

        // join -> on -> derived subquery
        let actual = table("Foo")
            .select()
            .join("Bar")
            .on("Foo.id = Bar.id")
            .alias_as("Sub")
            .select();
        let expected = "
            SELECT * FROM (
                SELECT * FROM Foo
                INNER JOIN Bar ON Foo.id = Bar.id
            ) Sub
            ";
        test_query_builder(actual, expected);
    }
}
