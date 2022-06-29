use {
    crate::{
        ast::{AstLiteral, Expr},
        parse_sql::parse_expr,
        result::{Error, Result},
        translate::translate_expr,
    },
    bigdecimal::BigDecimal,
};

#[derive(Clone)]
pub enum ExprNode {
    Expr(Expr),
    Text(String),
}

impl TryFrom<ExprNode> for Expr {
    type Error = Error;

    fn try_from(expr_node: ExprNode) -> Result<Self> {
        match expr_node {
            ExprNode::Expr(expr) => Ok(expr),
            ExprNode::Text(expr) => {
                let expr = parse_expr(expr)?;

                translate_expr(&expr)
            }
        }
    }
}

impl From<&str> for ExprNode {
    fn from(expr: &str) -> Self {
        ExprNode::Text(expr.to_owned())
    }
}

impl From<i64> for ExprNode {
    fn from(n: i64) -> Self {
        ExprNode::Expr(Expr::Literal(AstLiteral::Number(BigDecimal::from(n))))
    }
}
