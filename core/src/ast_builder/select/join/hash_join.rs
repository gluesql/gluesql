use {
    super::{JoinConstraintData, JoinOperatorType},
    crate::{
        ast::{Join, JoinExecutor},
        ast_builder::{
            select::{NodeData, Prebuild},
            ExprList, ExprNode, FilterNode, GroupByNode, JoinConstraintNode, JoinNode, LimitNode,
            OffsetNode, OrderByExprList, OrderByNode, ProjectNode, SelectItemList,
        },
        result::Result,
    },
};

#[derive(Clone)]
pub struct HashJoinNode {
    join_node: JoinNode,
    key_expr: ExprNode,
    value_expr: ExprNode,
    filter_expr: Option<ExprNode>,
}

impl HashJoinNode {
    pub fn new<T: Into<ExprNode>, U: Into<ExprNode>>(
        join_node: JoinNode,
        key_expr: T,
        value_expr: U,
    ) -> Self {
        Self {
            join_node,
            key_expr: key_expr.into(),
            value_expr: value_expr.into(),
            filter_expr: None,
        }
    }

    pub fn hash_filter<T: Into<ExprNode>>(mut self, expr: T) -> Self {
        let expr = expr.into();
        let filter_expr = match self.filter_expr {
            Some(filter_expr) => filter_expr.and(expr),
            None => expr,
        };

        self.filter_expr = Some(filter_expr);
        self
    }

    pub fn on<T: Into<ExprNode>>(self, expr: T) -> JoinConstraintNode {
        JoinConstraintNode::new(self, expr)
    }

    pub fn join(self, table_name: &str) -> JoinNode {
        JoinNode::new(self, table_name.to_owned(), None, JoinOperatorType::Inner)
    }

    pub fn join_as(self, table_name: &str, alias: &str) -> JoinNode {
        JoinNode::new(
            self,
            table_name.to_owned(),
            Some(alias.to_owned()),
            JoinOperatorType::Inner,
        )
    }

    pub fn left_join(self, table_name: &str) -> JoinNode {
        JoinNode::new(self, table_name.to_owned(), None, JoinOperatorType::Left)
    }

    pub fn left_join_as(self, table_name: &str, alias: &str) -> JoinNode {
        JoinNode::new(
            self,
            table_name.to_owned(),
            Some(alias.to_owned()),
            JoinOperatorType::Left,
        )
    }

    pub fn project<T: Into<SelectItemList>>(self, select_items: T) -> ProjectNode {
        ProjectNode::new(self, select_items)
    }

    pub fn group_by<T: Into<ExprList>>(self, expr_list: T) -> GroupByNode {
        GroupByNode::new(self, expr_list)
    }

    pub fn offset<T: Into<ExprNode>>(self, expr: T) -> OffsetNode {
        OffsetNode::new(self, expr)
    }

    pub fn limit<T: Into<ExprNode>>(self, expr: T) -> LimitNode {
        LimitNode::new(self, expr)
    }

    pub fn filter<T: Into<ExprNode>>(self, expr: T) -> FilterNode {
        FilterNode::new(self, expr)
    }

    pub fn order_by<T: Into<OrderByExprList>>(self, order_by_exprs: T) -> OrderByNode {
        OrderByNode::new(self, order_by_exprs)
    }

    pub fn prebuild_for_constraint(self) -> Result<JoinConstraintData> {
        let mut join_constraint_data = self.join_node.prebuild_for_constraint()?;

        join_constraint_data.executor =
            build_join_executor(self.key_expr, self.value_expr, self.filter_expr)?;

        Ok(join_constraint_data)
    }
}

impl Prebuild for HashJoinNode {
    fn prebuild(self) -> Result<NodeData> {
        let (mut select_data, relation, join_operator) = self.join_node.prebuild_for_hash_join()?;
        let join_executor = build_join_executor(self.key_expr, self.value_expr, self.filter_expr)?;

        let join = Join {
            relation,
            join_operator,
            join_executor,
        };

        select_data.joins.push(join);
        Ok(select_data)
    }
}

fn build_join_executor(
    key_expr: ExprNode,
    value_expr: ExprNode,
    filter_expr: Option<ExprNode>,
) -> Result<JoinExecutor> {
    Ok(JoinExecutor::Hash {
        key_expr: key_expr.try_into()?,
        value_expr: value_expr.try_into()?,
        where_clause: filter_expr.map(ExprNode::try_into).transpose()?,
    })
}

#[cfg(test)]
mod tests {
    use crate::{
        ast::{
            Join, JoinConstraint, JoinExecutor, JoinOperator, Query, Select, SetExpr, Statement,
            TableFactor, TableWithJoins,
        },
        ast_builder::{col, expr, table, Build, SelectItemList},
    };

    #[test]
    fn hash_join() {
        let actual = table("Player")
            .select()
            .join("PlayerItem")
            .hash_executor("PlayerItem.user_id", col("Player.id"))
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
                group_by: Vec::new(),
                having: None,
            };

            Ok(Statement::Query(Query {
                body: SetExpr::Select(Box::new(select)),
                order_by: Vec::new(),
                limit: None,
                offset: None,
            }))
        };
        assert_eq!(actual, expected, "without filter");

        let actual = table("Player")
            .select()
            .join("PlayerItem")
            .hash_executor("PlayerItem.user_id", "Player.id")
            .hash_filter("PlayerItem.amount > 10")
            .hash_filter("PlayerItem.amount * 3 <= 2")
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
                    where_clause: Some(
                        expr("PlayerItem.amount > 10 AND PlayerItem.amount * 3 <= 2")
                            .try_into()
                            .unwrap(),
                    ),
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
                group_by: Vec::new(),
                having: None,
            };

            Ok(Statement::Query(Query {
                body: SetExpr::Select(Box::new(select)),
                order_by: Vec::new(),
                limit: None,
                offset: None,
            }))
        };
        assert_eq!(actual, expected, "with filter");
    }
}
