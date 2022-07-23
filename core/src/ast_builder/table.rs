use super::{CreateIndexNode, DeleteNode, DropIndexNode, SelectNode};

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

    pub fn drop_index(self, name: &str) -> DropIndexNode {
        DropIndexNode::new(self.table_name, name.to_string())
    }

    pub fn create_index(self, name: &str) -> CreateIndexNode {
        CreateIndexNode::new(self.table_name, name.to_string())
    }
}
