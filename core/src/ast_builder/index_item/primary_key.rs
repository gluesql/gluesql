use {super::IndexItemNode, crate::ast_builder::ExprNode};

#[derive(Clone, Debug)]
pub struct PrimaryKeyNode;

impl<'a> PrimaryKeyNode {
    /// Create a Primary Key IndexItem
    pub fn into_primary_key<T: Into<ExprNode<'a>>>(self, expressions: Vec<T>) -> IndexItemNode<'a> {
        IndexItemNode::PrimaryKey(expressions.into_iter().map(Into::into).collect())
    }
}

/// Entry point function to Primary Key
pub fn primary_key() -> PrimaryKeyNode {
    PrimaryKeyNode
}

#[cfg(test)]
mod tests {
    use crate::{
        ast::{AstLiteral, Expr},
        ast_builder::{index_item::IndexItem, primary_key, select::Prebuild},
    };

    #[test]
    fn test() {
        let actual = primary_key()
            .into_primary_key(vec!["1"])
            .prebuild()
            .unwrap();
        let expected = IndexItem::PrimaryKey(vec![Expr::Literal(AstLiteral::Number(1.into()))]);
        assert_eq!(actual, expected);
    }
}
