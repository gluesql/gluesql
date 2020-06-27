mod error;
mod parsed;

use std::fmt::Debug;

use sqlparser::ast::{BinaryOperator, Expr, Value as AstValue};

use crate::executor::{select, FilterContext};
use crate::result::Result;
use crate::storage::Store;

pub use error::EvaluateError;
pub use parsed::Parsed;

pub fn evaluate<'a, T: 'static + Debug>(
    storage: &'a dyn Store<T>,
    filter_context: &'a FilterContext<'a>,
    expr: &'a Expr,
) -> Result<Parsed<'a>> {
    let eval = |expr| evaluate(storage, filter_context, expr);
    let parse_value = |value: &'a AstValue| match value {
        v @ AstValue::Number(_) => Ok(Parsed::LiteralRef(v)),
        _ => Err(EvaluateError::Unimplemented.into()),
    };

    match expr {
        Expr::Value(value) => parse_value(&value),
        Expr::Identifier(ident) => match ident.quote_style {
            Some(_) => Ok(Parsed::StringRef(&ident.value)),
            None => filter_context.get_value(&ident.value).map(Parsed::ValueRef),
        },
        Expr::Nested(expr) => eval(&expr),
        Expr::CompoundIdentifier(idents) => {
            if idents.len() != 2 {
                return Err(EvaluateError::UnsupportedCompoundIdentifier(expr.to_string()).into());
            }

            let table_alias = &idents[0].value;
            let column = &idents[1].value;

            filter_context
                .get_alias_value(table_alias, column)
                .map(Parsed::ValueRef)
        }
        Expr::Subquery(query) => select(storage, &query, Some(filter_context))?
            .map(|row| row?.take_first_value())
            .map(|value| value.map(Parsed::Value))
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
