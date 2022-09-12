use super::{
    CreateTableNode, DeleteNode, DropTableNode, InsertNode, SelectNode, ShowColumnsNode, UpdateNode,
};

#[cfg(feature = "alter-table")]
use super::AlterTableNode;

#[cfg(feature = "index")]
use super::{CreateIndexNode, DropIndexNode, OrderByExprNode};

#[derive(Clone)]
pub struct TableNode {
    pub table_name: String,
}

impl TableNode {
    pub fn select(self) -> SelectNode {
        SelectNode::new(self.table_name)
    }

    pub fn delete(self) -> DeleteNode {
        DeleteNode::new(self.table_name)
    }

    #[cfg(feature = "index")]
    pub fn drop_index(self, name: &str) -> DropIndexNode {
        DropIndexNode::new(self.table_name, name.to_string())
    }

    #[cfg(feature = "index")]
    pub fn create_index<T: Into<OrderByExprNode>>(self, name: &str, column: T) -> CreateIndexNode {
        CreateIndexNode::new(self.table_name, name.to_string(), column.into())
    }

    pub fn show_columns(self) -> ShowColumnsNode {
        ShowColumnsNode::new(self.table_name)
    }

    #[cfg(feature = "alter-table")]
    pub fn alter_table(self) -> AlterTableNode {
        AlterTableNode::new(self.table_name)
    }

    pub fn create_table(self) -> CreateTableNode {
        CreateTableNode::new(self.table_name, false)
    }

    pub fn create_table_if_not_exists(self) -> CreateTableNode {
        CreateTableNode::new(self.table_name, true)
    }

    pub fn drop_table(self) -> DropTableNode {
        DropTableNode::new(self.table_name, false)
    }

    pub fn drop_table_if_exists(self) -> DropTableNode {
        DropTableNode::new(self.table_name, true)
    }

    pub fn update(self) -> UpdateNode {
        UpdateNode::new(self.table_name)
    }

    pub fn insert(self) -> InsertNode {
        InsertNode::new(self.table_name)
    }
}
