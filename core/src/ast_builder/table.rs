use super::{DeleteNode, SelectNode};

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
    pub fn create_index(self, name: &str, column_name: &str, asc: bool) -> CreateIndexNode {
        CreateIndexNode::new(
            self.table_name,
            name.to_string(),
            OrderByExprNode::Text(column_name.to_string(), Some(asc)),
        )
    }
}
