use super::{DeleteNode, SelectNode, ShowColumnsNode};

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
    pub fn create_index(self, name: &str, column: &str) -> CreateIndexNode {
        CreateIndexNode::new(
            self.table_name,
            name.to_string(),
            OrderByExprNode::Text(column.to_string()),
        )
    }

    pub fn show_columns(self) -> ShowColumnsNode {
        ShowColumnsNode::new(self.table_name)
    }
}
