use {super::IndexItemNode, crate::ast_builder::ExprNode};

#[derive(Clone, Debug)]
pub struct PrimaryKeyNode;

impl<'a> PrimaryKeyNode {
    pub fn eq<T: Into<ExprNode<'a>>>(self, expr: T) -> IndexItemNode<'a> {
        IndexItemNode::PrimaryKey(expr.into())
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
        let actual = primary_key().eq("1").prebuild().unwrap();
        let expected = IndexItem::PrimaryKey(Expr::Literal(AstLiteral::Number(1.into())));
        assert_eq!(actual, expected);
    }
}
