use {
    super::{
        CreateTableNode, DeleteNode, DropTableNode, ExprNode, InsertNode, SelectNode,
        ShowColumnsNode, UpdateNode,
    },
    crate::ast::Dictionary,
};

#[cfg(feature = "alter-table")]
use super::AlterTableNode;

#[cfg(feature = "index")]
use super::{CreateIndexNode, DropIndexNode, OrderByExprNode};

#[derive(Clone)]
pub enum TableType<'a> {
    Table,
    Series(ExprNode<'a>),
    Dictionary(Dictionary),
    Derived {
        subquery: Box<SelectNode<'a>>,
        alias: String,
    },
}

#[derive(Clone)]
pub struct TableNode<'a> {
    pub table_name: String,
    pub table_type: TableType<'a>,
}

impl<'a> TableNode<'a> {
    pub fn alias_as(self, table_alias: &str) -> TableAliasNode<'a> {
        TableAliasNode {
            table_node: self,
            table_alias: table_alias.to_owned(),
        }
    }

    pub fn select(self) -> SelectNode<'a> {
        SelectNode::new(self, None)
    }

    pub fn delete(self) -> DeleteNode<'static> {
        DeleteNode::new(self.table_name)
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

    pub fn update(self) -> UpdateNode<'static> {
        UpdateNode::new(self.table_name)
    }

    pub fn insert(self) -> InsertNode {
        InsertNode::new(self.table_name)
    }
}

#[derive(Clone)]
pub struct TableAliasNode<'a> {
    pub table_node: TableNode<'a>,
    pub table_alias: String,
}

impl<'a> TableAliasNode<'a> {
    pub fn select(self) -> SelectNode<'a> {
        SelectNode::new(self.table_node, Some(self.table_alias))
    }
}
