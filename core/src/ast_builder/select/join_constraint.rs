use {
    super::{join::JoinOperatorType, NodeData, Prebuild},
    crate::{
        ast::{Join, JoinConstraint, JoinExecutor, JoinOperator},
        ast_builder::{
            ExprList, ExprNode, FilterNode, GroupByNode, JoinNode, LimitNode, OffsetNode,
            OrderByExprList, OrderByNode, ProjectNode, SelectItemList,
        },
        result::Result,
    },
};

#[derive(Clone)]
pub struct JoinConstraintNode {
    join_node: JoinNode,
    expr: ExprNode,
}

impl JoinConstraintNode {
    pub fn new<T: Into<ExprNode>>(join_node: JoinNode, expr: T) -> Self {
        Self {
            join_node,
            expr: expr.into(),
        }
    }

    pub fn join(self, table_name: &str) -> JoinNode {
        JoinNode::new(self, table_name.to_string(), None, JoinOperatorType::Inner)
    }

    pub fn join_as(self, table_name: &str, alias: &str) -> JoinNode {
        JoinNode::new(
            self,
            table_name.to_string(),
            Some(alias.to_string()),
            JoinOperatorType::Inner,
        )
    }

    pub fn left_join(self, table_name: &str) -> JoinNode {
        JoinNode::new(self, table_name.to_string(), None, JoinOperatorType::Left)
    }

    pub fn left_join_as(self, table_name: &str, alias: &str) -> JoinNode {
        JoinNode::new(
            self,
            table_name.to_string(),
            Some(alias.to_string()),
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
}

impl Prebuild for JoinConstraintNode {
    fn prebuild(self) -> Result<NodeData> {
        let (mut select_data, relation, join_operator_type) =
            self.join_node.prebuild_for_constraint()?;
        select_data.joins.push(Join {
            relation,
            join_operator: match join_operator_type {
                JoinOperatorType::Inner => {
                    JoinOperator::Inner(JoinConstraint::On(self.expr.try_into()?))
                }
                JoinOperatorType::Left => {
                    JoinOperator::LeftOuter(JoinConstraint::On(self.expr.try_into()?))
                }
            },
            join_executor: JoinExecutor::NestedLoop,
        });
        Ok(select_data)
    }
}

#[cfg(test)]
mod tests {
    use crate::ast_builder::{table, test, Build};

    #[test]
    fn join_constraint() {
        // join node ->  join constarint node -> build
        let actual = table("Foo")
            .select()
            .join("Bar")
            .on("Foo.id = Bar.id")
            .build();
        let expected = "SELECT * FROM Foo INNER JOIN Bar ON Foo.id = Bar.id";
        test(actual, expected);

        // join node ->  join constraint node -> build
        let actual = table("Foo")
            .select()
            .join_as("Bar", "B")
            .on("Foo.id = B.id")
            .build();
        let expected = "SELECT * FROM Foo INNER JOIN Bar B ON Foo.id = B.id";
        test(actual, expected);

        // join node -> join constraint node -> build
        let actual = table("Foo")
            .select()
            .left_join("Bar")
            .on("Foo.id = Bar.id")
            .build();
        let expected = "SELECT * FROM Foo LEFT OUTER JOIN Bar ON Foo.id = Bar.id";
        test(actual, expected);

        // join node -> join constraint node -> build
        let actual = table("Foo")
            .select()
            .left_join_as("Bar", "b")
            .on("Foo.id = b.id")
            .build();
        let expected = "SELECT * FROM Foo LEFT OUTER JOIN Bar b ON Foo.id = b.id";
        test(actual, expected);
    }
}
