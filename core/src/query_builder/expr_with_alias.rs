use crate::{ast::Expr, plan::ExprPlan, query_builder::ExprNode, result::Result};

#[derive(Clone, Debug)]
pub struct ExprWithAliasNode<'a> {
    pub expr: ExprNode<'a>,
    pub alias: String,
}

impl ExprWithAliasNode<'_> {
    pub(super) fn build_expr_with_alias_plan(self) -> Result<(ExprPlan, String)> {
        Ok((self.expr.build_expr_plan()?, self.alias))
    }

    pub(super) fn build_expr_with_alias(self) -> Result<(Expr, String)> {
        Ok((self.expr.build_expr()?, self.alias))
    }
}
