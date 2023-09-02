use super::{CmpExprNode, IndexItemNode};

#[derive(Clone, Debug)]
pub struct OrderNode<'a> {
    prev_node: CmpExprNode<'a>,
    asc: bool,
}

impl<'a> OrderNode<'a> {
    pub fn new(prev_node: CmpExprNode<'a>, asc: bool) -> Self {
        Self { prev_node, asc }
    }

    pub fn build(self) -> IndexItemNode<'a> {
        IndexItemNode::NonClustered {
            name: self.prev_node.index_name,
            asc: Some(self.asc),
            cmp_expr: Some((self.prev_node.operator, self.prev_node.expr)),
        }
    }
}
