use crate::{
    ast::Expr,
    ast_builder::ExprNode,
    result::{Error, Result},
};

#[derive(Clone, Debug)]
pub struct ExprWithAliasNode<'a> {
    pub expr: ExprNode<'a>,
    pub alias: String,
}

impl<'a> TryFrom<ExprWithAliasNode<'a>> for (Expr, String) {
    type Error = Error;

    fn try_from(node: ExprWithAliasNode<'a>) -> Result<Self> {
        Ok((Expr::try_from(node.expr)?, node.alias))
    }
}
