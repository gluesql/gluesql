use std::vec;

use {
    super::{NodeData, Prebuild},
    crate::{
        ast::{ObjectName, SelectItem, Statement, TableFactor},
        ast_builder::{
            ExprList, ExprNode, FilterNode, GroupByNode, JoinNode, LimitNode, OffsetNode,
            ProjectNode, SelectItemList,
        },
        result::Result,
    },
};

pub enum JoinType {
    Inner,
    Left,
}

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

    pub fn build(self) -> Result<Statement> {
        self.prebuild().map(NodeData::build_stmt)
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
            filters: None,
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
    use crate::ast_builder::{table, test};

    #[test]
    fn select() {
        // Select to build
        let actual = table("App").select().build();
        let expected = "SELECT * FROM App";
        test(actual, expected);

        // select node to filter node
        let actual = table("Bar").select().filter("id IS NULL").build();
        let expected = "SELECT * FROM Bar WHERE id IS NULL";
        test(actual, expected);

        // select node to group by node
        let actual = table("Foo").select().group_by("id").build();
        let expected = "SELECT * FROM Foo GROUP BY id";
        test(actual, expected);

        // select node to offset node
        let actual = table("Foo").select().offset(1).build();
        let expected = "SELECT * FROM Foo OFFSET 1";
        test(actual, expected);

        // select node to limit node
        let actual = table("Foo").select().limit(1).build();
        let expected = "SELECT * FROM Foo LIMIT 1";
        test(actual, expected);

        // select node to project node
        let actual = table("Foo").select().project(vec!["id", "name"]).build();
        let expected = "SELECT id, name FROM Foo";
        test(actual, expected);

        // select node to join node
        let actual = table("Foo").select().join("Bar").build();
        let expected = "SELECT * FROM Foo JOIN Bar";
        test(actual, expected);

        // select node to join node with alias
        let actual = table("Foo").select().join_as("Bar", "b").build();
        let expected = "SELECT * FROM Foo JOIN Bar AS b";
        test(actual, expected);

        // select node to left join node
        let actual = table("Foo").select().left_join("Bar").build();
        let expected = "SELECT * FROM Foo LEFT JOIN Bar";
        test(actual, expected);

        // select node to left join node with alias
        let actual = table("Foo").select().left_join_as("Bar", "b").build();
        let expected = "SELECT * FROM Foo LEFT JOIN Bar AS b";
        test(actual, expected);
    }
}
