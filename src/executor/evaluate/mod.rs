mod error;
mod evaluated;
mod expr;
mod stateless;

use {
    super::{context::FilterContext, select::select},
    crate::{
        ast::{Aggregate, Expr, Function},
        data::Value,
        result::{Error, Result},
        store::GStore,
    },
    async_recursion::async_recursion,
    futures::stream::{self, StreamExt, TryStreamExt},
    im_rc::HashMap,
    std::{
        borrow::Cow,
        convert::{TryFrom, TryInto},
        fmt::Debug,
        rc::Rc,
    },
};

pub use {error::EvaluateError, evaluated::Evaluated, stateless::evaluate_stateless};

#[async_recursion(?Send)]
pub async fn evaluate<'a, T: 'static + Debug>(
    storage: &'a dyn GStore<T>,
    context: Option<Rc<FilterContext<'a>>>,
    aggregated: Option<Rc<HashMap<&'a Aggregate, Value>>>,
    expr: &'a Expr,
) -> Result<Evaluated<'a>> {
    let eval = |expr| {
        let context = context.as_ref().map(Rc::clone);
        let aggregated = aggregated.as_ref().map(Rc::clone);

        evaluate(storage, context, aggregated, expr)
    };

    match expr {
        Expr::Literal(ast_literal) => expr::literal(ast_literal),
        Expr::TypedString { data_type, value } => {
            expr::typed_string(data_type, Cow::Borrowed(value))
        }
        Expr::Identifier(ident) => {
            let context = context.ok_or(EvaluateError::UnreachableEmptyContext)?;

            match context.get_value(ident) {
                Some(value) => Ok(value.clone()),
                None => Err(EvaluateError::ValueNotFound(ident.to_string()).into()),
            }
            .map(Evaluated::from)
        }
        Expr::Nested(expr) => eval(expr).await,
        Expr::CompoundIdentifier(idents) => {
            if idents.len() != 2 {
                return Err(EvaluateError::UnsupportedCompoundIdentifier(expr.clone()).into());
            }

            let table_alias = &idents[0];
            let column = &idents[1];
            let context = context.ok_or(EvaluateError::UnreachableEmptyContext)?;

            match context.get_alias_value(table_alias, column) {
                Some(value) => Ok(value.clone()),
                None => Err(EvaluateError::ValueNotFound(column.to_string()).into()),
            }
            .map(Evaluated::from)
        }
        Expr::Subquery(query) => select(storage, query, context.as_ref().map(Rc::clone))
            .await?
            .map_ok(|row| row.take_first_value().map(Evaluated::from))
            .take(1)
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .next()
            .unwrap_or_else(|| Err(EvaluateError::NestedSelectRowNotFound.into()))?,
        Expr::BinaryOp { op, left, right } => {
            let left = eval(left).await?;
            let right = eval(right).await?;

            expr::binary_op(op, left, right)
        }
        Expr::UnaryOp { op, expr } => {
            let v = eval(expr).await?;

            expr::unary_op(op, v)
        }
        Expr::Aggregate(aggr) => match aggregated
            .as_ref()
            .map(|aggregated| aggregated.get(aggr.as_ref()))
            .flatten()
        {
            Some(value) => Ok(Evaluated::from(value.clone())),
            None => Err(EvaluateError::UnreachableEmptyAggregateValue(*aggr.clone()).into()),
        },
        Expr::Function(func) => {
            let context = context.as_ref().map(Rc::clone);
            let aggregated = aggregated.as_ref().map(Rc::clone);

            evaluate_function(storage, context, aggregated, func).await
        }
        Expr::Cast { expr, data_type } => eval(expr).await?.cast(data_type),
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let negated = *negated;
            let target = eval(expr).await?;

            stream::iter(list.iter())
                .filter_map(|expr| {
                    let target = &target;

                    async move {
                        eval(expr).await.map_or_else(
                            |error| Some(Err(error)),
                            |evaluated| (target == &evaluated).then(|| Ok(!negated)),
                        )
                    }
                })
                .take(1)
                .collect::<Vec<_>>()
                .await
                .into_iter()
                .next()
                .unwrap_or(Ok(negated))
                .map(Value::Bool)
                .map(Evaluated::from)
        }
        Expr::InSubquery {
            expr,
            subquery,
            negated,
        } => {
            let target = eval(expr).await?;

            select(storage, subquery, context)
                .await?
                .try_filter_map(|row| {
                    let target = &target;

                    async move {
                        let value = row.take_first_value()?;

                        (target == &Evaluated::from(&value))
                            .then(|| Ok(!negated))
                            .transpose()
                    }
                })
                .take(1)
                .collect::<Vec<_>>()
                .await
                .into_iter()
                .next()
                .unwrap_or(Ok(*negated))
                .map(Value::Bool)
                .map(Evaluated::from)
        }
        Expr::Between {
            expr,
            negated,
            low,
            high,
        } => {
            let target = eval(expr).await?;
            let low = eval(low).await?;
            let high = eval(high).await?;

            expr::between(target, *negated, low, high)
        }
        Expr::Exists(query) => {
            let v = select(storage, query, context)
                .await?
                .into_stream()
                .take(1)
                .try_collect::<Vec<_>>()
                .await?
                .get(0)
                .is_some();

            Ok(Evaluated::from(Value::Bool(v)))
        }
        Expr::IsNull(expr) => {
            let v = eval(expr).await?.is_null();

            Ok(Evaluated::from(Value::Bool(v)))
        }
        Expr::IsNotNull(expr) => {
            let v = eval(expr).await?.is_null();

            Ok(Evaluated::from(Value::Bool(!v)))
        }
        Expr::Wildcard | Expr::QualifiedWildcard(_) => {
            Err(EvaluateError::UnreachableWildcardExpr.into())
        }
    }
}

async fn evaluate_function<'a, T: 'static + Debug>(
    storage: &'a dyn GStore<T>,
    context: Option<Rc<FilterContext<'a>>>,
    aggregated: Option<Rc<HashMap<&'a Aggregate, Value>>>,
    func: &'a Function,
) -> Result<Evaluated<'a>> {
    let eval = |expr| {
        let context = context.as_ref().map(Rc::clone);
        let aggregated = aggregated.as_ref().map(Rc::clone);

        evaluate(storage, context, aggregated, expr)
    };

    enum Nullable<T> {
        Value(T),
        Null,
    }

    let eval_to_str = |name: &'static str, expr| async move {
        match eval(expr).await?.try_into()? {
            Value::Str(s) => Ok(Nullable::Value(s)),
            Value::Null => Ok(Nullable::Null),
            _ => {
                Err::<_, Error>(EvaluateError::FunctionRequiresStringValue(name.to_owned()).into())
            }
        }
    };

    let eval_to_float = |name: &'a str, expr| async move {
        match eval(expr).await?.try_into()? {
            Value::I64(v) => Ok(Nullable::Value(v as f64)),
            Value::F64(v) => Ok(Nullable::Value(v)),
            Value::Null => Ok(Nullable::Null),
            _ => Err::<_, Error>(EvaluateError::FunctionRequiresFloatValue(name.to_owned()).into()),
        }
    };

    match func {
        Function::Lower(expr) => match eval_to_str("LOWER", expr).await? {
            Nullable::Value(v) => Ok(Value::Str(v.to_lowercase())),
            Nullable::Null => Ok(Value::Null),
        }
        .map(Evaluated::from),
        Function::Upper(expr) => match eval_to_str("UPPER", expr).await? {
            Nullable::Value(v) => Ok(Value::Str(v.to_uppercase())),
            Nullable::Null => Ok(Value::Null),
        }
        .map(Evaluated::from),
        Function::Left { expr, size } | Function::Right { expr, size } => {
            let name = if matches!(func, Function::Left { .. }) {
                "LEFT"
            } else {
                "RIGHT"
            };

            let string = match eval_to_str(name, expr).await? {
                Nullable::Value(v) => v,
                Nullable::Null => {
                    return Ok(Evaluated::from(Value::Null));
                }
            };

            let size = match eval(size).await?.try_into()? {
                Value::I64(number) => usize::try_from(number)
                    .map_err(|_| EvaluateError::FunctionRequiresUSizeValue(name.to_owned()))?,
                Value::Null => {
                    return Ok(Evaluated::from(Value::Null));
                }
                _ => {
                    return Err(EvaluateError::FunctionRequiresIntegerValue(name.to_owned()).into());
                }
            };

            let converted = if name == "LEFT" {
                string.get(..size).map(|v| v.to_string()).unwrap_or(string)
            } else {
                let start_pos = if size > string.len() {
                    0
                } else {
                    string.len() - size
                };

                string
                    .get(start_pos..)
                    .map(|value| value.to_string())
                    .unwrap_or(string)
            };

            Ok(Evaluated::from(Value::Str(converted)))
        }
        Function::Ceil(expr) => match eval_to_float("CEIL", expr).await? {
            Nullable::Value(v) => Ok(Value::F64(v.ceil())),
            Nullable::Null => Ok(Value::Null),
        }
        .map(Evaluated::from),
        Function::Round(expr) => match eval_to_float("ROUND", expr).await? {
            Nullable::Value(v) => Ok(Value::F64(v.round())),
            Nullable::Null => Ok(Value::Null),
        }
        .map(Evaluated::from),
        Function::Floor(expr) => match eval_to_float("FLOOR", expr).await? {
            Nullable::Value(v) => Ok(Value::F64(v.floor())),
            Nullable::Null => Ok(Value::Null),
        }
        .map(Evaluated::from),
        Function::Trim(expr) => match eval_to_str("TRIM", expr).await? {
            Nullable::Value(string) => Ok(Value::Str(string.trim().to_owned())),
            Nullable::Null => Ok(Value::Null),
        }
        .map(Evaluated::from),
        Function::Sin(expr) | Function::Cos(expr) | Function::Tan(expr) => {
            let float_number = eval_to_float(func.name(), expr).await?;

            let trigonometric = |func, value| match func {
                Function::Sin(_) => f64::sin(value),
                Function::Cos(_) => f64::cos(value),
                Function::Tan(_) => f64::tan(value),
                _ => panic!("Unexpected function: {:?}", func.name()),
            };

            match float_number {
                Nullable::Value(v) => Ok(Value::F64(trigonometric(func.to_owned(), v))),
                Nullable::Null => Ok(Value::Null),
            }
            .map(Evaluated::from)
        }
    }
}
