use {super::IndexItemNode, crate::ast_builder::ExprNode};

#[derive(Clone, Debug)]
pub struct PrimaryKeyCmpExprNode<'a> {
    pub expr: ExprNode<'a>,
}

impl<'a> PrimaryKeyCmpExprNode<'a> {
    pub fn new<T: Into<ExprNode<'a>>>(expr: T) -> Self {
        Self { expr: expr.into() }
    }
    pub fn build(self) -> IndexItemNode<'a> {
        IndexItemNode::PrimaryKey(self.expr)
    }
}

#[cfg(test)]
mod tests {
    use crate::ast_builder::{index_item::IndexItem, primary_key, select::Prebuild, to_expr};

    #[test]
    fn test() {
        let actual = primary_key().eq("1").build().prebuild().unwrap();
        let expected = IndexItem::PrimaryKey(to_expr("1"));
        assert_eq!(actual, expected);
    }
}
