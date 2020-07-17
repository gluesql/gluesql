mod error;
mod evaluated;

use std::fmt::Debug;

use sqlparser::ast::{BinaryOperator, Expr, Value as AstValue};

use super::context::FilterContext;
use super::select::select;
use crate::result::Result;
use crate::storage::Store;

pub use error::EvaluateError;
pub use evaluated::Evaluated;

pub fn evaluate<'a, T: 'static + Debug>(
    storage: &'a dyn Store<T>,
    filter_context: Option<&'a FilterContext<'a>>,
    expr: &'a Expr,
) -> Result<Evaluated<'a>> {
    let eval = |expr| evaluate(storage, filter_context, expr);

    match expr {
        Expr::Value(value) => match value {
            AstValue::Number(_)
            | AstValue::Boolean(_)
            | AstValue::SingleQuotedString(_)
            | AstValue::Null => Ok(Evaluated::LiteralRef(value)),
            _ => Err(EvaluateError::Unimplemented.into()),
        },
        Expr::Identifier(ident) => match ident.quote_style {
            Some(_) => Ok(Evaluated::StringRef(&ident.value)),
            None => filter_context
                .ok_or_else(|| {
                    let name = ident.value.to_string();

                    EvaluateError::UnreachableEmptyFilterContext(name)
                })?
                .get_value(&ident.value)
                .map(Evaluated::ValueRef),
        },
        Expr::Nested(expr) => eval(&expr),
        Expr::CompoundIdentifier(idents) => {
            if idents.len() != 2 {
                return Err(EvaluateError::UnsupportedCompoundIdentifier(expr.to_string()).into());
            }

            let table_alias = &idents[0].value;
            let column = &idents[1].value;

            filter_context
                .ok_or_else(|| {
                    let name = format!("{}.{}", table_alias, column);

                    EvaluateError::UnreachableEmptyFilterContext(name)
                })?
                .get_alias_value(table_alias, column)
                .map(Evaluated::ValueRef)
        }
        Expr::Subquery(query) => select(storage, &query, filter_context)?
            .map(|row| row?.take_first_value())
            .map(|value| value.map(Evaluated::Value))
            .next()
            .ok_or(EvaluateError::NestedSelectRowNotFound)?,
        Expr::BinaryOp { op, left, right } => {
            let l = eval(left)?;
            let r = eval(right)?;

            match op {
                BinaryOperator::Plus => l.add(&r),
                BinaryOperator::Minus => l.subtract(&r),
                BinaryOperator::Multiply => l.multiply(&r),
                BinaryOperator::Divide => l.divide(&r),
                _ => Err(EvaluateError::Unimplemented.into()),
            }
        }
        _ => Err(EvaluateError::Unimplemented.into()),
    }
}
