use crate::{ast::IndexItem, ast_builder::ExprNode};

#[derive(Clone, Debug)]
pub struct PrimaryKeyNode;

impl<'a> PrimaryKeyNode {
    pub fn eq<T: Into<ExprNode<'a>>>(self, expr: T) -> Option<IndexItem> {
        Some(IndexItem::PrimaryKey(expr.into().try_into().ok()?))
    }
}

/// Entry point function to Primary Key
pub fn primary_key() -> PrimaryKeyNode {
    PrimaryKeyNode
}
