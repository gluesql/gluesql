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
pub enum ExprList {
    Text(String),
    Exprs(Vec<ExprNode>),
}

impl From<&str> for ExprList {
    fn from(exprs: &str) -> Self {
        ExprList::Text(exprs.to_owned())
    }
}

impl From<Vec<ExprNode>> for ExprList {
    fn from(exprs: Vec<ExprNode>) -> Self {
        ExprList::Exprs(exprs)
    }
}

impl From<Vec<&str>> for ExprList {
    fn from(exprs: Vec<&str>) -> Self {
        ExprList::Exprs(exprs.into_iter().map(Into::into).collect())
    }
}

impl TryFrom<ExprList> for Vec<Expr> {
    type Error = Error;

    fn try_from(expr_list: ExprList) -> Result<Self> {
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
