use crate::{
    ast::Expr,
    parse_sql::parse_comma_separated_exprs,
    result::{Error, Result},
    translate::translate_expr,
};

#[derive(Clone)]
pub enum ExprList {
    Text(String),
}

impl From<&str> for ExprList {
    fn from(exprs: &str) -> Self {
        ExprList::Text(exprs.to_owned())
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
        }
    }
}
