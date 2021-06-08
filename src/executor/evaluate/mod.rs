mod error;
mod evaluated;
mod expr;
mod stateless;

use {
    super::{context::FilterContext, select::select},
    crate::{
        ast::{Expr, Function, FunctionArg},
        data::{get_name, Value},
        result::Result,
        store::GStore,
    },
    async_recursion::async_recursion,
    boolinator::Boolinator,
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
    aggregated: Option<Rc<HashMap<&'a Function, Value>>>,
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
            expr::typed_string(data_type, Cow::Borrowed(&value))
        }
        Expr::Identifier(ident) => {
            let context = context.ok_or(EvaluateError::UnreachableEmptyContext)?;

            match context.get_value(&ident) {
                Some(value) => Ok(value.clone()),
                None => Err(EvaluateError::ValueNotFound(ident.to_string()).into()),
            }
            .map(Evaluated::from)
        }
        Expr::Nested(expr) => eval(&expr).await,
        Expr::CompoundIdentifier(idents) => {
            if idents.len() != 2 {
                return Err(
                    EvaluateError::UnsupportedCompoundIdentifier(format!("{:?}", expr)).into(),
                );
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
        Expr::Subquery(query) => select(storage, &query, context.as_ref().map(Rc::clone))
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
        Expr::Function(func) => match aggregated.as_ref().map(|aggr| aggr.get(func)).flatten() {
            Some(value) => Ok(Evaluated::from(value.clone())),
            None => {
                let context = context.as_ref().map(Rc::clone);
                let aggregated = aggregated.as_ref().map(Rc::clone);

                evaluate_function(storage, context, aggregated, func).await
            }
        },
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
                            |evaluated| (target == &evaluated).as_some(Ok(!negated)),
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

            select(storage, &subquery, context)
                .await?
                .try_filter_map(|row| {
                    let target = &target;

                    async move {
                        let value = row.take_first_value()?;

                        (target == &Evaluated::from(&value))
                            .as_some(Ok(!negated))
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
    aggregated: Option<Rc<HashMap<&'a Function, Value>>>,
    func: &'a Function,
) -> Result<Evaluated<'a>> {
    let eval = |expr| {
        let context = context.as_ref().map(Rc::clone);
        let aggregated = aggregated.as_ref().map(Rc::clone);

        evaluate(storage, context, aggregated, expr)
    };

    let get_arg_expr = |arg: &'a FunctionArg| -> Result<&'a Expr> {
        match arg {
            FunctionArg::Unnamed(expr) => Ok(expr),
            FunctionArg::Named { name, .. } => {
                Err(EvaluateError::UnreachableFunctionArg(name.to_string()).into())
            }
        }
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

            let arg = match args.len() {
                1 => &args[0],
                found => {
                    return Err(EvaluateError::NumberOfFunctionParamsNotMatching {
                        expected: 1,
                        found,
                    }
                    .into());
                }
            };

            let expr = get_arg_expr(arg)?;
            let value: Value = match eval(expr).await?.try_into()? {
                Value::Str(s) => Value::Str(convert(s)),
                Value::Null => Value::Null,
                _ => {
                    return Err(EvaluateError::FunctionRequiresStringValue(name.to_string()).into());
                }
            };

            Ok(Evaluated::from(value))
        }
        name @ "LEFT" | name @ "RIGHT" => {
            match args.len() {
                2 => (),
                found => {
                    return Err(EvaluateError::NumberOfFunctionParamsNotMatching {
                        expected: 2,
                        found,
                    }
                    .into())
                }
            }

            let string = match eval(get_arg_expr(&args[0])?).await?.try_into()? {
                Value::Str(string) => Ok(string),
                Value::Null => {
                    return Ok(Evaluated::from(Value::Null));
                }
                _ => Err(EvaluateError::FunctionRequiresStringValue(name.to_string())),
            }?;

            let number = match eval(get_arg_expr(&args[1])?).await?.try_into()? {
                Value::I64(number) => usize::try_from(number)
                    .map_err(|_| EvaluateError::FunctionRequiresUSizeValue(name.to_string())), // Unlikely to occur hence the imperfect error
                Value::Null => {
                    return Ok(Evaluated::from(Value::Null));
                }
                _ => Err(EvaluateError::FunctionRequiresIntegerValue(
                    name.to_string(),
                )),
            }?;

            let converted = {
                if name == "LEFT" {
                    string.get(..number)
                } else {
                    let start_pos = if number > string.len() {
                        0
                    } else {
                        string.len() - number
                    };

                    string.get(start_pos..)
                }
                .map(|value| value.to_string())
                .unwrap_or(string)
            };

            Ok(Evaluated::from(Value::Str(converted)))
        }
        name => Err(EvaluateError::FunctionNotSupported(name.to_owned()).into()),
    }
}
