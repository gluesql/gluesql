use {super::ExprNode, crate::ast_builder::ExprWithAliasNode};

impl<'a> ExprNode<'a> {
    pub fn alias_as(self, alias: &str) -> ExprWithAliasNode<'a> {
        ExprWithAliasNode {
            expr: self,
            alias: alias.to_owned(),
        }
    }
}
