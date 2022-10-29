use crate::{
    ast::FormatType,
    ast_builder::ExprNode,
    result::{Error, Result},
};

#[derive(Clone)]
pub enum FormatTypeNode<'a> {
    Datetime(ExprNode<'a>),
    Binary,
    Hex,
}

impl<'a> TryFrom<FormatTypeNode<'a>> for FormatType {
    type Error = Error;

    fn try_from(value: FormatTypeNode) -> Result<Self> {
        match value {
            FormatTypeNode::Datetime(expr) => {
                let expr = expr.try_into()?;
                Ok(FormatType::Datetime(expr))
            }
            FormatTypeNode::Binary => Ok(FormatType::Binary),
            FormatTypeNode::Hex => Ok(FormatType::Hex),
        }
    }
}

impl<'a> From<ExprNode<'a>> for FormatTypeNode<'a> {
    fn from(expr: ExprNode<'a>) -> Self {
        Self::Datetime(expr)
    }
}
