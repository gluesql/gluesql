mod error;
mod evaluated;

use im_rc::HashMap;
use std::convert::TryInto;
use std::fmt::Debug;
use std::rc::Rc;

use sqlparser::ast::{BinaryOperator, UnaryOperator, Expr, Function, Value as AstValue};

use super::context::FilterContext;
use super::select::select;
use crate::data::{get_name, Value};
use crate::result::Result;
use crate::store::Store;

pub use error::EvaluateError;
pub use evaluated::Evaluated;

pub fn evaluate<'a, T: 'static + Debug>(
    storage: &'a dyn Store<T>,
    context: Option<Rc<FilterContext<'a>>>,
    aggregated: Option<&HashMap<&Function, Value>>,
    expr: &'a Expr,
) -> Result<Evaluated<'a>> {
    let eval = |expr| {
        let context = context.as_ref().map(Rc::clone);

        evaluate(storage, context, aggregated, expr)
    };

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
            // TODO: remove panic!
            None => match context {
                None => {
                    panic!();
                }
                Some(context) => context.get_value(&ident.value).map(|value| match value {
                    Some(value) => Evaluated::ValueRef(value),
                    None => Evaluated::Value(Value::Empty),
                }),
            },
        },
        Expr::Nested(expr) => eval(&expr),
        Expr::CompoundIdentifier(idents) => {
            if idents.len() != 2 {
                return Err(EvaluateError::UnsupportedCompoundIdentifier(expr.to_string()).into());
            }

            let table_alias = &idents[0].value;
            let column = &idents[1].value;

            // TODO: remove panic!
            match context {
                None => {
                    panic!();
                }
                Some(context) => {
                    context
                        .get_alias_value(table_alias, column)
                        .map(|value| match value {
                            Some(value) => Evaluated::ValueRef(value),
                            None => Evaluated::Value(Value::Empty),
                        })
                }
            }
        }
        Expr::Subquery(query) => select(storage, &query, context.as_ref().map(Rc::clone))?
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
        Expr::UnaryOp { op, expr } => {
            let v = eval(expr)?;

            match op {
                UnaryOperator::Plus => v.positive(),
                UnaryOperator::Minus => v.negative(),
                _ => Err(EvaluateError::Unimplemented.into()),
            }
        }
        Expr::Function(func) => aggregated
            .as_ref()
            .map(|aggr| aggr.get(func))
            .flatten()
            .map_or_else(
                || {
                    let context = context.as_ref().map(Rc::clone);

                    evaluate_function(storage, context, aggregated, func)
                },
                |value| Ok(Evaluated::Value(value.clone())),
            ),
        _ => Err(EvaluateError::Unimplemented.into()),
    }
}

fn evaluate_function<'a, T: 'static + Debug>(
    storage: &'a dyn Store<T>,
    context: Option<Rc<FilterContext<'a>>>,
    aggregated: Option<&HashMap<&Function, Value>>,
    func: &'a Function,
) -> Result<Evaluated<'a>> {
    let eval = |expr| {
        let context = context.as_ref().map(Rc::clone);

        evaluate(storage, context, aggregated, expr)
    };

    let Function { name, args, .. } = func;

    match get_name(name)?.to_uppercase().as_str() {
        name @ "LOWER" | name @ "UPPER" => {
            let convert = |s: String| {
                if name == "LOWER" {
                    s.to_lowercase()
                } else {
                    s.to_uppercase()
                }
            };

            if args.len() != 1 {
                return Err(EvaluateError::NumberOfFunctionParamsNotMatching {
                    expected: 1,
                    found: args.len(),
                }
                .into());
            }

            let expr = &args[0];
            let value: Value = match eval(expr)?.try_into()? {
                Value::Str(s) => Value::Str(convert(s)),
                Value::OptStr(s) => Value::OptStr(s.map(convert)),
                _ => {
                    return Err(EvaluateError::FunctionRequiresStringValue(name.to_string()).into());
                }
            };

            Ok(Evaluated::Value(value))
        }
        name => Err(EvaluateError::FunctionNotSupported(name.to_owned()).into()),
    }
}
