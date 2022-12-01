use super::{
    table_factor::TableType, CreateTableNode, DeleteNode, DropTableNode, InsertNode, SelectNode,
    ShowColumnsNode, TableFactorNode, UpdateNode,
};

#[cfg(feature = "alter-table")]
use super::AlterTableNode;

#[cfg(feature = "index")]
use super::{CreateIndexNode, DropIndexNode, OrderByExprNode};

#[derive(Clone)]
pub struct TableNameNode {
    pub table_name: String,
}

impl<'a> TableNameNode {
    fn next(self) -> TableFactorNode<'a> {
        TableFactorNode {
            table_name: self.table_name.clone(),
            table_type: TableType::Table,
            table_alias: None,
        }
    }

    pub fn select(self) -> SelectNode<'a> {
        SelectNode::new(self.next())
    }

    // todo: is it okay to get just string?
    pub fn delete(self) -> DeleteNode<'static> {
        DeleteNode::new(self.table_name)
    }

    pub fn update(self) -> UpdateNode<'static> {
        UpdateNode::new(self.table_name)
    }

    pub fn insert(self) -> InsertNode {
        InsertNode::new(self.table_name)
    }

    pub fn show_columns(self) -> ShowColumnsNode {
        ShowColumnsNode::new(self.table_name)
    }

    pub fn alias_as(self, table_alias: &str) -> TableFactorNode<'a> {
        TableFactorNode {
            table_name: self.table_name,
            table_type: TableType::Table,
            table_alias: Some(table_alias.to_owned()),
        }
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

    #[cfg(feature = "index")]
    pub fn drop_index(self, name: &str) -> DropIndexNode {
        DropIndexNode::new(self.table_name, name.to_owned())
    }

    #[cfg(feature = "index")]
    pub fn create_index<T: Into<OrderByExprNode<'a>>>(
        self,
        name: &str,
        column: T,
    ) -> CreateIndexNode<'a> {
        CreateIndexNode::new(self.table_name, name.to_owned(), column.into())
    }

    #[cfg(feature = "alter-table")]
    pub fn alter_table(self) -> AlterTableNode {
        AlterTableNode::new(self.table_name)
    }
}
