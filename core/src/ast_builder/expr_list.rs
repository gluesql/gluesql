use {
    super::ExprNode,
    crate::{
        ast::Expr,
        parse_sql::parse_comma_separated_exprs,
        result::{Error, Result},
        translate::translate_expr,
    },
};

#[derive(Clone)]
pub enum ExprList<'a> {
    Text(String),
    Exprs(Vec<ExprNode<'a>>),
}

impl<'a> From<&str> for ExprList<'a> {
    fn from(exprs: &str) -> Self {
        ExprList::Text(exprs.to_owned())
    }
}

impl<'a> From<Vec<ExprNode<'a>>> for ExprList<'a> {
    fn from(exprs: Vec<ExprNode<'a>>) -> Self {
        ExprList::Exprs(exprs)
    }
}

impl<'a> From<Vec<&str>> for ExprList<'a> {
    fn from(exprs: Vec<&str>) -> Self {
        ExprList::Exprs(exprs.into_iter().map(Into::into).collect())
    }
}

impl<'a> TryFrom<ExprList<'a>> for Vec<Expr> {
    type Error = Error;

    fn try_from(expr_list: ExprList<'a>) -> Result<Self> {
        match expr_list {
            ExprList::Text(exprs) => parse_comma_separated_exprs(exprs)?
                .iter()
                .map(translate_expr)
                .collect::<Result<Vec<_>>>(),
            ExprList::Exprs(nodes) => nodes
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<Vec<_>>>(),
        }
    }
}
