use super::{DeleteNode, DropTableNode, SelectNode, ShowColumnsNode};

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

    pub fn show_columns(self) -> ShowColumnsNode {
        ShowColumnsNode::new(self.table_name)
    }

    pub fn drop_table(self) -> DropTableNode {
        DropTableNode::new(self.table_name, false)
    }

    pub fn drop(self) -> DropTableNode {
        DropTableNode::new(self.table_name, false)
    }

    pub fn drop_exists(self) -> DropTableNode {
        DropTableNode::new(self.table_name, true)
    }

    pub fn drop_exists_table(self) -> DropTableNode {
        DropTableNode::new(self.table_name, true)
    }
}
