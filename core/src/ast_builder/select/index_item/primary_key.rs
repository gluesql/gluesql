use {
    super::use_index::UseIndexNode,
    crate::{ast::IndexItem, ast_builder::ExprNode},
};

#[derive(Clone, Debug)]
pub enum PrevNode {
    UseIndex(UseIndexNode),
}

impl From<UseIndexNode> for PrevNode {
    fn from(node: UseIndexNode) -> Self {
        PrevNode::UseIndex(node)
    }
}

#[derive(Clone, Debug)]
pub struct PrimaryKeyNode<'a> {
    prev_node: PrevNode,
    expr: ExprNode<'a>,
}

impl<'a> PrimaryKeyNode<'a> {
    pub fn new<N: Into<PrevNode>, T: Into<ExprNode<'a>>>(prev_node: N, expr: T) -> Self {
        Self {
            prev_node: prev_node.into(),
            expr: expr.into(),
        }
    }

    pub fn build(self) -> IndexItem {
        match self.prev_node {
            PrevNode::UseIndex(_) => IndexItem::PrimaryKey(self.expr.try_into().unwrap()),
        }
    }
}
