use {
    super::primary_key::PrimaryKeyNode,
    crate::{ast::IndexItem, ast_builder::ExprNode},
};

#[derive(Clone, Debug)]
pub struct UseIndexNode;

impl<'a> UseIndexNode {
    pub fn primary_key<T: Into<ExprNode<'a>>>(self, expr: T) -> IndexItem {
        PrimaryKeyNode::new(self, expr).build()
    }
}

/// Entry point function to Index Item
pub fn use_idx() -> UseIndexNode {
    UseIndexNode
}
