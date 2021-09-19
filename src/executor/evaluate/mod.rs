mod error;
mod evaluated;
mod expr;
mod stateless;

use {
    super::{context::FilterContext, select::select},
    crate::{
        ast::{Aggregate, Expr, Function, TrimWhereField},
        data::Value,
        result::{Error, Result},
        store::GStore,
    },
    async_recursion::async_recursion,
    futures::{
        future::ready,
        stream::{self, StreamExt, TryStreamExt},
    },
    im_rc::HashMap,
    itertools::Itertools,
    std::{
        borrow::Cow,
        cmp::{max, min},
        convert::{TryFrom, TryInto},
        fmt::Debug,
        mem::discriminant,
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
            .next()
            .await
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

            stream::iter(list)
                .then(eval)
                .try_filter(|evaluated| ready(evaluated == &target))
                .try_next()
                .await
                .map(|v| v.is_some() ^ negated)
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
                .and_then(|row| ready(row.take_first_value().map(Evaluated::from)))
                .try_filter(|evaluated| ready(evaluated == &target))
                .try_next()
                .await
                .map(|v| v.is_some() ^ negated)
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
        Expr::Exists(query) => select(storage, query, context)
            .await?
            .try_next()
            .await
            .map(|v| v.is_some())
            .map(Value::Bool)
            .map(Evaluated::from),
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
        Expr::Case {
            operand,
            when_then,
            else_result,
        } => {
            let when_then: Vec<(Evaluated, Evaluated)> = stream::iter(when_then.iter())
                .map(Ok::<_, Error>)
                .and_then(|(when, then)| async move { Ok((eval(when).await?, eval(then).await?)) })
                .try_collect()
                .await?;

            let else_result = match else_result {
                Some(result) => Some(eval(result).await?),
                None => None,
            };

            let parse_pair =
                |result_pair: (Result<Value>, Result<Value>)| -> Result<(Value, Value)> {
                    match result_pair {
                        (Err(e), _) | (_, Err(e)) => Err(e),
                        _ => Ok((result_pair.0.unwrap(), result_pair.1.unwrap())),
                    }
                };

            let when_then_val = when_then
                .clone()
                .into_iter()
                .map(|(when, then)| parse_pair((when.try_into(), then.try_into())))
                .collect::<Result<Vec<(Value, Value)>>>()?;

            if let Some(er) = else_result {
                let else_result = er.try_into()?;
                let then_type = when_then_val
                    .iter()
                    .map(|(_, t)| (discriminant(t)))
                    .unique()
                    .collect_vec();

                if let Some(then_type_first) = then_type.first() {
                    if then_type_first != &discriminant(&else_result) || then_type.len() != 1 {
                        return Err(EvaluateError::UnequalResultTypes("CASE".to_owned()).into());
                    }
                }
            }

            match operand {
                Some(op) => {
                    let op = eval(op).await?;
                    Ok(when_then
                        .into_iter()
                        .find_map(|(when, then)| when.eq(&op).then(|| then))
                        .unwrap_or_else(|| Evaluated::from(Value::Null)))
                }
                None => when_then_val
                    .into_iter()
                    .map(|(when, then)| match when {
                        Value::Bool(condition) => Ok((condition, then)),
                        _ => Err(EvaluateError::BooleanTypeRequired("CASE".to_owned()).into()),
                    })
                    .collect::<Result<Vec<(bool, Value)>>>()
                    .map_or_else(Err::<_, Error>, |wt| {
                        Ok(wt
                            .into_iter()
                            .find_map(|(when, then)| when.then(|| Evaluated::from(then)))
                            .unwrap_or_else(|| Evaluated::from(Value::Null)))
                    }),
            }
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

    let eval_to_str = |expr| async move {
        match eval(expr).await?.try_into()? {
            Value::Str(s) => Ok(Nullable::Value(s)),
            Value::Null => Ok(Nullable::Null),
            _ => {
                Err::<_, Error>(EvaluateError::FunctionRequiresStringValue(func.to_string()).into())
            }
        }
    };

    let eval_to_float = |expr| async move {
        match eval(expr).await?.try_into()? {
            Value::I64(v) => Ok(Nullable::Value(v as f64)),
            Value::F64(v) => Ok(Nullable::Value(v)),
            Value::Null => Ok(Nullable::Null),
            _ => {
                Err::<_, Error>(EvaluateError::FunctionRequiresFloatValue(func.to_string()).into())
            }
        }
    };

    let eval_to_integer = |expr| async move {
        match eval(expr).await?.try_into()? {
            Value::I64(number) => Ok(Nullable::Value(number)),
            Value::Null => Ok(Nullable::Null),
            _ => Err::<_, Error>(
                EvaluateError::FunctionRequiresIntegerValue(func.to_string()).into(),
            ),
        }
    };

    match func {
        Function::Lower(expr) => match eval_to_str(expr).await? {
            Nullable::Value(v) => Ok(Value::Str(v.to_lowercase())),
            Nullable::Null => Ok(Value::Null),
        }
        .map(Evaluated::from),
        Function::Upper(expr) => match eval_to_str(expr).await? {
            Nullable::Value(v) => Ok(Value::Str(v.to_uppercase())),
            Nullable::Null => Ok(Value::Null),
        }
        .map(Evaluated::from),

        Function::Sqrt(expr) => match eval_to_float(expr).await? {
            Nullable::Value(v) => Ok(Value::F64(v.sqrt())),
            Nullable::Null => Ok(Value::Null),
        }
        .map(Evaluated::from),

        Function::Power { expr, power } => {
            let number = match eval_to_float(expr).await? {
                Nullable::Value(v) => v as f64,
                Nullable::Null => {
                    return Ok(Evaluated::from(Value::Null));
                }
            };

            let power = match eval_to_float(power).await? {
                Nullable::Value(v) => v as f64,
                Nullable::Null => {
                    return Ok(Evaluated::from(Value::Null));
                }
            };

            Ok(Evaluated::from(Value::F64(number.powf(power) as f64)))
        }

        Function::Left { expr, size } | Function::Right { expr, size } => {
            let name = if matches!(func, Function::Left { .. }) {
                "LEFT"
            } else {
                "RIGHT"
            };

            let string = match eval_to_str(expr).await? {
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
        Function::ASin(expr) | Function::ACos(expr) | Function::ATan(expr) => {
            let float_number = eval_to_float(expr).await?;

            let trigonometric = |func, value| match func {
                Function::ASin(_) => f64::asin(value),
                Function::ACos(_) => f64::acos(value),
                _ => f64::atan(value),
            };

            match float_number {
                Nullable::Value(v) => Ok(Value::F64(trigonometric(func.to_owned(), v))),
                Nullable::Null => Ok(Value::Null),
            }
            .map(Evaluated::from)
        }
        Function::Lpad { expr, size, fill } | Function::Rpad { expr, size, fill } => {
            let name = if matches!(func, Function::Lpad { .. }) {
                "LPAD"
            } else {
                "RPAD"
            };

            let string = match eval_to_str(expr).await? {
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

            let fill = match fill {
                Some(expr) => match eval_to_str(expr).await? {
                    Nullable::Value(v) => v,
                    Nullable::Null => {
                        return Ok(Evaluated::from(Value::Null));
                    }
                },
                None => " ".to_string(),
            };

            let result = if size > string.len() {
                let padding_size = size - string.len();
                let repeat_count = padding_size / fill.len();
                let plus_count = padding_size % fill.len();
                let fill = fill.repeat(repeat_count) + &fill[0..plus_count];

                if name == "LPAD" {
                    fill + &string
                } else {
                    string + &fill
                }
            } else {
                string[0..size].to_string()
            };

            Ok(Evaluated::from(Value::Str(result)))
        }
        Function::Ceil(expr) => match eval_to_float(expr).await? {
            Nullable::Value(v) => Ok(Value::F64(v.ceil())),
            Nullable::Null => Ok(Value::Null),
        }
        .map(Evaluated::from),
        Function::Round(expr) => match eval_to_float(expr).await? {
            Nullable::Value(v) => Ok(Value::F64(v.round())),
            Nullable::Null => Ok(Value::Null),
        }
        .map(Evaluated::from),
        Function::Floor(expr) => match eval_to_float(expr).await? {
            Nullable::Value(v) => Ok(Value::F64(v.floor())),
            Nullable::Null => Ok(Value::Null),
        }
        .map(Evaluated::from),
        Function::Radians(expr) => match eval_to_float(expr).await? {
            Nullable::Value(v) => Ok(Value::F64(v.to_radians())),
            Nullable::Null => Ok(Value::Null),
        }
        .map(Evaluated::from),
        Function::Degrees(expr) => match eval_to_float(expr).await? {
            Nullable::Value(v) => Ok(Value::F64(v.to_degrees())),
            Nullable::Null => Ok(Value::Null),
        }
        .map(Evaluated::from),
        Function::Pi() => {
            { Ok(Evaluated::from(Value::F64(std::f64::consts::PI))) }.map(Evaluated::from)
        }
        Function::Trim {
            expr,
            filter_chars,
            trim_where_field,
        } => {
            let expr_str = match eval_to_str(expr).await? {
                Nullable::Value(str) => str,
                Nullable::Null => return Ok(Value::Null).map(Evaluated::from),
            };
            let expr_str = expr_str.as_str();

            let filter_chars = match filter_chars {
                Some(expr) => match eval_to_str(expr).await? {
                    Nullable::Value(str) => str.chars().collect::<Vec<_>>(),
                    Nullable::Null => return Ok(Evaluated::from(Value::Null)),
                },
                None => vec![' '],
            };

            Ok(Value::Str(
                match trim_where_field {
                    Some(TrimWhereField::Both) => expr_str.trim_matches(&filter_chars[..]),
                    Some(TrimWhereField::Leading) => expr_str.trim_start_matches(&filter_chars[..]),
                    Some(TrimWhereField::Trailing) => expr_str.trim_end_matches(&filter_chars[..]),
                    None => return Ok(Evaluated::from(Value::Str(expr_str.trim().to_owned()))),
                }
                .to_owned(),
            ))
            .map(Evaluated::from)
        }
        Function::Exp(expr) => match eval_to_float(expr).await? {
            Nullable::Value(v) => Ok(Value::F64(v.exp())),
            Nullable::Null => Ok(Value::Null),
        }
        .map(Evaluated::from),
        Function::Ln(expr) => match eval_to_float(expr).await? {
            Nullable::Value(v) => Ok(Value::F64(v.ln())),
            Nullable::Null => Ok(Value::Null),
        }
        .map(Evaluated::from),
        Function::Log2(expr) => match eval_to_float(expr).await? {
            Nullable::Value(v) => Ok(Value::F64(v.log2())),
            Nullable::Null => Ok(Value::Null),
        }
        .map(Evaluated::from),
        Function::Log10(expr) => match eval_to_float(expr).await? {
            Nullable::Value(v) => Ok(Value::F64(v.log10())),
            Nullable::Null => Ok(Value::Null),
        }
        .map(Evaluated::from),
        Function::Sin(expr) | Function::Cos(expr) | Function::Tan(expr) => {
            let float_number = eval_to_float(expr).await?;

            let trigonometric = |func, value| match func {
                Function::Sin(_) => f64::sin(value),
                Function::Cos(_) => f64::cos(value),
                _ => f64::tan(value),
            };

            match float_number {
                Nullable::Value(v) => Ok(Value::F64(trigonometric(func.to_owned(), v))),
                Nullable::Null => Ok(Value::Null),
            }
            .map(Evaluated::from)
        }
        Function::Div { dividend, divisor } => {
            let name = "DIV";

            let dividend = match eval(dividend).await?.try_into()? {
                Value::F64(number) => number,
                Value::I64(number) => number as f64,
                Value::Null => {
                    return Ok(Evaluated::from(Value::Null));
                }
                _ => {
                    return Err(EvaluateError::FunctionRequiresFloatOrIntegerValue(
                        name.to_owned(),
                    )
                    .into());
                }
            };

            let divisor = match eval(divisor).await?.try_into()? {
                Value::F64(number) => match number {
                    x if x == 0.0 => return Err(EvaluateError::DivisorShouldNotBeZero.into()),
                    _ => number,
                },
                Value::I64(number) => match number {
                    0 => return Err(EvaluateError::DivisorShouldNotBeZero.into()),
                    _ => number as f64,
                },
                Value::Null => {
                    return Ok(Evaluated::from(Value::Null));
                }
                _ => {
                    return Err(EvaluateError::FunctionRequiresFloatOrIntegerValue(
                        name.to_owned(),
                    )
                    .into());
                }
            };

            Ok(Evaluated::from(Value::I64((dividend / divisor) as i64)))
        }
        Function::Mod { dividend, divisor } => {
            let dividend = eval(dividend).await?;
            let divisor = eval(divisor).await?;
            dividend.modulo(&divisor)
        }
        Function::Gcd { left, right } => {
            let left = match eval_to_integer(left).await? {
                Nullable::Value(v) => v,
                Nullable::Null => {
                    return Ok(Evaluated::from(Value::Null));
                }
            };
            let right = match eval_to_integer(right).await? {
                Nullable::Value(v) => v,
                Nullable::Null => {
                    return Ok(Evaluated::from(Value::Null));
                }
            };

            Ok(Evaluated::from(Value::I64(gcd(left, right))))
        }
        Function::Lcm { left, right } => {
            let left = match eval_to_integer(left).await? {
                Nullable::Value(v) => v,
                Nullable::Null => {
                    return Ok(Evaluated::from(Value::Null));
                }
            };
            let right = match eval_to_integer(right).await? {
                Nullable::Value(v) => v,
                Nullable::Null => {
                    return Ok(Evaluated::from(Value::Null));
                }
            };

            fn lcm(a: i64, b: i64) -> i64 {
                a * b / gcd(a, b)
            }

            Ok(Evaluated::from(Value::I64(lcm(left, right))))
        }
        Function::Ltrim { expr, chars } | Function::Rtrim { expr, chars } => {
            let name = if matches!(func, Function::Ltrim { .. }) {
                "LTRIM"
            } else {
                "RTRIM"
            };
            let pattern: Result<Vec<char>> = match chars {
                Some(chars) => match eval_to_str(chars).await? {
                    Nullable::Value(v) => Ok(v.chars().collect::<Vec<char>>()),
                    Nullable::Null => {
                        return Ok(Evaluated::from(Value::Null));
                    }
                },
                None => Ok(" ".chars().collect::<Vec<char>>()),
            };
            match eval_to_str(expr).await? {
                Nullable::Value(v) => {
                    if name == "LTRIM" {
                        Ok(Value::Str(v.trim_start_matches(&pattern?[..]).to_string()))
                    } else {
                        Ok(Value::Str(v.trim_end_matches(&pattern?[..]).to_string()))
                    }
                }
                Nullable::Null => Ok(Value::Null),
            }
            .map(Evaluated::from)
        }
        Function::Reverse(expr) => {
            match eval_to_str(expr).await? {
                Nullable::Value(v) => Ok(Value::Str(v.chars().rev().collect::<String>())),
                Nullable::Null => Ok(Value::Null),
            }
        }
        .map(Evaluated::from),

        Function::Substr { expr, start, count } => {
            let string = match eval_to_str(expr).await? {
                Nullable::Value(v) => v,
                Nullable::Null => {
                    return Ok(Evaluated::from(Value::Null));
                }
            };
            let start = match eval_to_integer(start).await? {
                Nullable::Value(v) => v - 1,
                Nullable::Null => return Ok(Evaluated::from(Value::Null)),
            };

            let end = match count {
                Some(expr) => match eval_to_integer(expr).await? {
                    Nullable::Value(count) => {
                        if count < 0 {
                            return Err(EvaluateError::NegativeSubstrLenNotAllowed.into());
                        }
                        min(max(start + count, 0) as usize, string.len())
                    }
                    Nullable::Null => return Ok(Evaluated::from(Value::Null)),
                },
                None => string.len(),
            };

            let start = min(max(start, 0) as usize, string.len());
            let string = String::from(&string[start..end]);
            Ok(Evaluated::from(Value::Str(string)))
        }
    }
}

fn gcd(a: i64, b: i64) -> i64 {
    if b == 0 {
        a
    } else {
        gcd(b, a % b)
    }
}
