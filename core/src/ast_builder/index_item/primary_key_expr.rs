use crate::{
    ast::IndexItem,
    ast_builder::{insert::Expr, ExprNode},
};

#[derive(Clone, Debug)]
pub struct PrimaryKeyCmpExprNode {
    pub expr: Expr,
}

impl<'a> PrimaryKeyCmpExprNode {
    pub fn new(expr: ExprNode<'a>) -> Self {
        Self {
            expr: expr.try_into().ok().unwrap(),
        }
    }
    pub fn build(self) -> IndexItem {
        IndexItem::PrimaryKey(self.expr)
    }
}

#[cfg(test)]
mod tests {
    use {
        super::IndexItem,
        crate::ast_builder::{primary_key, to_expr},
    };

    #[test]
    fn test() {
        let actual = primary_key().eq("1").build();
        let expected = IndexItem::PrimaryKey(to_expr("1"));

        assert_eq!(actual, expected);
    }
}
