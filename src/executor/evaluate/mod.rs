mod error;
mod evaluated;

use {
    super::{context::FilterContext, select::select},
    crate::{
        data::{get_name, Literal, Value},
        result::Result,
        store::Store,
    },
    async_recursion::async_recursion,
    boolinator::Boolinator,
    futures::stream::{self, StreamExt, TryStreamExt},
    im_rc::HashMap,
    sqlparser::ast::{BinaryOperator, Expr, Function, FunctionArg, UnaryOperator},
    std::{
        borrow::Cow,
        convert::{TryFrom, TryInto},
        fmt::Debug,
        rc::Rc,
    },
};

pub use {error::EvaluateError, evaluated::Evaluated};

#[async_recursion(?Send)]
pub async fn evaluate<'a, T: 'static + Debug>(
    storage: &'a dyn Store<T>,
    context: Option<Rc<FilterContext<'a>>>,
    aggregated: Option<Rc<HashMap<&'a Function, Value>>>,
    expr: &'a Expr,
    use_empty: bool,
) -> Result<Evaluated<'a>> {
    let eval = |expr| {
        let context = context.as_ref().map(Rc::clone);
        let aggregated = aggregated.as_ref().map(Rc::clone);

        evaluate(storage, context, aggregated, expr, use_empty)
    };

    match expr {
        Expr::Value(value) => Literal::try_from(value).map(Evaluated::Literal),
        Expr::Identifier(ident) => match ident.quote_style {
            Some(_) => Ok(Literal::Text(Cow::Borrowed(&ident.value))).map(Evaluated::Literal),
            None => {
                let context = context.ok_or(EvaluateError::UnreachableEmptyContext)?;

                match (context.get_value(&ident.value), use_empty) {
                    (Some(value), _) => Ok(value.clone()),
                    (None, true) => Ok(Value::Null),
                    (None, false) => {
                        Err(EvaluateError::ValueNotFound(ident.value.to_string()).into())
                    }
                }
                .map(Evaluated::from)
            }
        },
        Expr::Nested(expr) => eval(&expr).await,
        Expr::CompoundIdentifier(idents) => {
            if idents.len() != 2 {
                return Err(EvaluateError::UnsupportedCompoundIdentifier(expr.to_string()).into());
            }

            let table_alias = &idents[0].value;
            let column = &idents[1].value;
            let context = context.ok_or(EvaluateError::UnreachableEmptyContext)?;

            match (context.get_alias_value(table_alias, column), use_empty) {
                (Some(value), _) => Ok(value.clone()),
                (None, true) => Ok(Value::Null),
                (None, false) => Err(EvaluateError::ValueNotFound(column.to_string()).into()),
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
            let l = eval(left).await?;
            let r = eval(right).await?;

            macro_rules! cmp {
                ($expr: expr) => {
                    Ok(Evaluated::from(Value::Bool($expr)))
                };
            }

            macro_rules! cond {
                (l $op: tt r) => {{
                    let l: bool = l.try_into()?;
                    let r: bool = r.try_into()?;
                    let v = l $op r;

                    Ok(Evaluated::from(Value::Bool(v)))
                }};
            }

            match op {
                BinaryOperator::Plus => l.add(&r),
                BinaryOperator::Minus => l.subtract(&r),
                BinaryOperator::Multiply => l.multiply(&r),
                BinaryOperator::Divide => l.divide(&r),
                BinaryOperator::StringConcat => l.concat(r),
                BinaryOperator::Eq => cmp!(l == r),
                BinaryOperator::NotEq => cmp!(l != r),
                BinaryOperator::Lt => cmp!(l < r),
                BinaryOperator::LtEq => cmp!(l <= r),
                BinaryOperator::Gt => cmp!(l > r),
                BinaryOperator::GtEq => cmp!(l >= r),
                BinaryOperator::And => cond!(l && r),
                BinaryOperator::Or => cond!(l || r),
                _ => Err(EvaluateError::Unimplemented.into()),
            }
        }
        Expr::UnaryOp { op, expr } => {
            let v = eval(expr).await?;

            match op {
                UnaryOperator::Plus => v.unary_plus(),
                UnaryOperator::Minus => v.unary_minus(),
                UnaryOperator::Not => v.try_into().map(|v: bool| Evaluated::from(Value::Bool(!v))),
                _ => Err(EvaluateError::Unimplemented.into()),
            }
        }
        Expr::Function(func) => match aggregated.as_ref().map(|aggr| aggr.get(func)).flatten() {
            Some(value) => Ok(Evaluated::from(value.clone())),
            None => {
                let context = context.as_ref().map(Rc::clone);
                let aggregated = aggregated.as_ref().map(Rc::clone);

                evaluate_function(storage, context, aggregated, func, use_empty).await
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
            let negated = *negated;
            let target = eval(expr).await?;

            let v = eval(low).await? <= target && target <= eval(high).await?;
            let v = negated ^ v;

            Ok(Evaluated::from(Value::Bool(v)))
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
        _ => Err(EvaluateError::Unimplemented.into()),
    }
}

async fn evaluate_function<'a, T: 'static + Debug>(
    storage: &'a dyn Store<T>,
    context: Option<Rc<FilterContext<'a>>>,
    aggregated: Option<Rc<HashMap<&'a Function, Value>>>,
    func: &'a Function,
    use_empty: bool,
) -> Result<Evaluated<'a>> {
    let eval = |expr| {
        let context = context.as_ref().map(Rc::clone);
        let aggregated = aggregated.as_ref().map(Rc::clone);

        evaluate(storage, context, aggregated, expr, use_empty)
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
