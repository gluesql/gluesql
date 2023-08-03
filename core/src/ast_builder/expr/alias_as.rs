use {super::ExprNode, std::borrow::Cow};

impl<'a> ExprNode<'a> {
    pub fn alias_as(self, alias: &str) -> ExprWithAliasNode<'a> {
        ExprWithAliasNode {
            expr: self,
            alias: alias.to_owned().into(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct ExprWithAliasNode<'a> {
    pub expr: ExprNode<'a>,
    pub alias: String,
}

impl<'a> From<&'a str> for ExprWithAliasNode<'a> {
    fn from(expr: &'a str) -> Self {
        ExprWithAliasNode {
            expr: ExprNode::SqlExpr(Cow::Borrowed(expr)),
            alias: expr.to_string(),
        }
    }
}
