use {super::CmpExprNode, crate::ast::IndexItem};

#[derive(Clone, Debug)]
pub struct OrderNode {
    prev_node: CmpExprNode,
    asc: bool,
}

impl OrderNode {
    pub fn new(prev_node: CmpExprNode, asc: bool) -> Self {
        Self { prev_node, asc }
    }

    pub fn build(self) -> IndexItem {
        IndexItem::NonClustered {
            name: self.prev_node.index_name,
            asc: Some(self.asc),
            cmp_expr: Some((self.prev_node.operator, self.prev_node.expr)),
        }
    }
}
