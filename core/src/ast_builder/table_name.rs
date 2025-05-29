use super::{
    AlterTableNode, CreateIndexNode, CreateTableNode, DeleteNode, DropIndexNode, DropTableNode,
    IndexItemNode, InsertNode, OrderByExprNode, SelectNode, ShowColumnsNode, TableFactorNode,
    UpdateNode, table_factor::TableType,
};
#[derive(Clone, Debug)]
pub struct TableNameNode {
    pub table_name: String,
}

impl<'a> TableNameNode {
    pub fn select(self) -> SelectNode<'a> {
        let table_factor = TableFactorNode {
            table_name: self.table_name,
            table_type: TableType::Table,
            table_alias: None,
            index: None,
        };

        SelectNode::new(table_factor)
    }

    pub fn delete(self) -> DeleteNode<'static> {
        DeleteNode::new(self.table_name)
    }

    pub fn update(self) -> UpdateNode {
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
            index: None,
        }
    }

    pub fn create_table(self) -> CreateTableNode {
        CreateTableNode::new(self.table_name, false)
    }

    pub fn create_table_if_not_exists(self) -> CreateTableNode {
        CreateTableNode::new(self.table_name, true)
    }

    pub fn drop_table(self) -> DropTableNode {
        DropTableNode::new(self.table_name, false, false)
    }

    pub fn drop_table_if_exists(self) -> DropTableNode {
        DropTableNode::new(self.table_name, true, false)
    }

    pub fn drop_table_cascade(self) -> DropTableNode {
        DropTableNode::new(self.table_name, false, true)
    }

    pub fn drop_table_if_exists_cascade(self) -> DropTableNode {
        DropTableNode::new(self.table_name, true, true)
    }

    pub fn drop_index(self, name: &str) -> DropIndexNode {
        DropIndexNode::new(self.table_name, name.to_owned())
    }

    pub fn create_index<T: Into<OrderByExprNode<'a>>>(
        self,
        name: &str,
        column: T,
    ) -> CreateIndexNode<'a> {
        CreateIndexNode::new(self.table_name, name.to_owned(), column.into())
    }

    pub fn alter_table(self) -> AlterTableNode {
        AlterTableNode::new(self.table_name)
    }

    pub fn index_by<T: Into<IndexItemNode<'a>>>(self, index_item: T) -> TableFactorNode<'a> {
        TableFactorNode {
            table_name: self.table_name,
            table_type: TableType::Table,
            table_alias: None,
            index: Some(index_item.into()),
        }
    }
}

/// Entry point function to build statement
pub fn table(table_name: &str) -> TableNameNode {
    let table_name = table_name.to_owned();

    TableNameNode { table_name }
}
