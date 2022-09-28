use {
    super::{join::JoinOperatorType, NodeData, Prebuild},
    crate::{
        ast::{ObjectName, SelectItem, TableFactor},
        ast_builder::{
            ExprList, ExprNode, FilterNode, GroupByNode, JoinNode, LimitNode, OffsetNode,
            OrderByExprList, OrderByNode, ProjectNode, SelectItemList,
        },
        result::Result,
    },
};

#[derive(Clone)]
pub struct SelectNode {
    table_name: String,
}

impl SelectNode {
    pub fn new(table_name: String) -> Self {
        Self { table_name }
    }

    pub fn filter<T: Into<ExprNode>>(self, expr: T) -> FilterNode {
        FilterNode::new(self, expr)
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

    pub fn project<T: Into<SelectItemList>>(self, select_items: T) -> ProjectNode {
        ProjectNode::new(self, select_items)
    }

    pub fn order_by<T: Into<OrderByExprList>>(self, order_by_exprs: T) -> OrderByNode {
        OrderByNode::new(self, order_by_exprs)
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
}

impl Prebuild for SelectNode {
    fn prebuild(self) -> Result<NodeData> {
        let relation = TableFactor::Table {
            name: ObjectName(vec![self.table_name]),
            alias: None,
            index: None,
        };

        Ok(NodeData {
            projection: vec![SelectItem::Wildcard],
            relation,
            filter: None,
            group_by: vec![],
            having: None,
            order_by: vec![],
            offset: None,
            limit: None,
            joins: vec![],
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::ast_builder::{table, test, Build};

    #[test]
    fn select() {
        // select node -> build
        let actual = table("App").select().build();
        let expected = "SELECT * FROM App";
        test(actual, expected);
    }
}
