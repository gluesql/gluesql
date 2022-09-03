use {
    super::{join::JoinType, NodeData, Prebuild},
    crate::{
        ast::{Join, JoinConstraint, JoinExecutor, JoinOperator, Statement, TableFactor},
        ast_builder::{
            ExprList, ExprNode, FilterNode, GroupByNode, JoinNode, LimitNode, OffsetNode,
            ProjectNode, SelectItemList,
        },
        result::Result,
    },
};

#[derive(Clone)]
pub enum PrevNode {
    Join(JoinNode),
}

impl Prebuild for PrevNode {
    fn prebuild(self) -> Result<NodeData> {
        match self {
            Self::Join(node) => node.prebuild(),
        }
    }
}

impl From<JoinNode> for PrevNode {
    fn from(node: JoinNode) -> Self {
        PrevNode::Join(node)
    }
}

#[derive(Clone)]
pub struct JoinConstraintNode {
    prev_node: PrevNode,
    relation: TableFactor,
    join_operator: JoinOperator,
}

impl JoinConstraintNode {
    pub fn new<N: Into<PrevNode>, K: Into<TableFactor>, T: Into<ExprNode>>(
        prev_node: N,
        relation: K,
        join_type: JoinType,
        expr: T,
    ) -> Self {
        Self {
            prev_node: prev_node.into(),
            relation: relation.into(),
            join_operator: match join_type {
                JoinType::Inner => {
                    JoinOperator::Inner(JoinConstraint::On(match expr.into().try_into() {
                        Ok(expr) => expr,
                        Err(err) => panic!("Problem in Change exprnode to expr: {:?}", err),
                    }))
                }
                JoinType::Left => {
                    JoinOperator::LeftOuter(JoinConstraint::On(match expr.into().try_into() {
                        Ok(expr) => expr,
                        Err(err) => panic!("Problem in Change exprnode to expr: {:?}", err),
                    }))
                }
            },
        }
    }

    pub fn join(self, table_name: &str) -> JoinNode {
        JoinNode::new(self, table_name.to_string(), None, JoinType::Inner)
    }

    pub fn join_as(self, table_name: &str, alias: &str) -> JoinNode {
        JoinNode::new(
            self,
            table_name.to_string(),
            Some(alias.to_string()),
            JoinType::Inner,
        )
    }

    pub fn left_join(self, table_name: &str) -> JoinNode {
        JoinNode::new(self, table_name.to_string(), None, JoinType::Left)
    }

    pub fn left_join_as(self, table_name: &str, alias: &str) -> JoinNode {
        JoinNode::new(
            self,
            table_name.to_string(),
            Some(alias.to_string()),
            JoinType::Left,
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

    pub fn build(self) -> Result<Statement> {
        self.prebuild().map(NodeData::build_stmt)
    }
}

impl Prebuild for JoinConstraintNode {
    fn prebuild(self) -> Result<NodeData> {
        let mut select_data = self.prev_node.prebuild()?;
        select_data.joins.pop();
        select_data.joins.push(Join {
            relation: self.relation,
            join_operator: self.join_operator,
            join_executor: JoinExecutor::NestedLoop,
        });
        Ok(select_data)
    }
}

#[cfg(test)]
mod tests {
    use crate::ast_builder::{table, test};

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
