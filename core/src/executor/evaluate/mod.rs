mod error;
mod evaluated;
mod expr;
mod function;

use {
    super::{context::RowContext, select::select},
    crate::{
        ast::{Aggregate, Expr, Function},
        data::{CustomFunction, Interval, Literal, Row, Value},
        mock::MockStorage,
        result::{Error, Result},
        store::GStore,
    },
    async_recursion::async_recursion,
    chrono::prelude::Utc,
    futures::{
        future::{ready, try_join_all},
        stream::{self, StreamExt, TryStreamExt},
    },
    im_rc::HashMap,
    std::{borrow::Cow, rc::Rc},
};

pub use {error::EvaluateError, evaluated::Evaluated};

#[async_recursion(?Send)]
pub async fn evaluate<'a, 'b: 'a, 'c: 'a, T: GStore>(
    storage: &'a T,
    context: Option<Rc<RowContext<'b>>>,
    aggregated: Option<Rc<HashMap<&'c Aggregate, Value>>>,
    expr: &'a Expr,
) -> Result<Evaluated<'a>> {
    evaluate_inner(Some(storage), context, aggregated, expr).await
}

pub async fn evaluate_stateless<'a, 'b: 'a>(
    context: Option<RowContext<'b>>,
    expr: &'a Expr,
) -> Result<Evaluated<'a>> {
    let context = context.map(Rc::new);
    let storage: Option<&MockStorage> = None;

    evaluate_inner(storage, context, None, expr).await
}

#[async_recursion(?Send)]
async fn evaluate_inner<'a, 'b: 'a, 'c: 'a, T: GStore>(
    storage: Option<&'a T>,
    context: Option<Rc<RowContext<'b>>>,
    aggregated: Option<Rc<HashMap<&'c Aggregate, Value>>>,
    expr: &'a Expr,
) -> Result<Evaluated<'a>> {
    let eval = |expr| {
        let context = context.as_ref().map(Rc::clone);
        let aggregated = aggregated.as_ref().map(Rc::clone);

        evaluate_inner(storage, context, aggregated, expr)
    };

    match expr {
        Expr::Literal(ast_literal) => expr::literal(ast_literal),
        Expr::TypedString { data_type, value } => {
            expr::typed_string(data_type, Cow::Borrowed(value))
        }
        Expr::Identifier(ident) => {
            let context = context
                .ok_or_else(|| EvaluateError::ContextRequiredForIdentEvaluation(expr.clone()))?;

            match context.get_value(ident) {
                Some(value) => Ok(value.clone()),
                None => Err(EvaluateError::ValueNotFound(ident.to_owned()).into()),
            }
            .map(Evaluated::from)
        }
        Expr::Nested(expr) => eval(expr).await,
        Expr::CompoundIdentifier { alias, ident } => {
            let table_alias = &alias;
            let column = &ident;
            let context = context
                .ok_or_else(|| EvaluateError::ContextRequiredForIdentEvaluation(expr.clone()))?;

            match context.get_alias_value(table_alias, column) {
                Some(value) => Ok(value.clone()),
                None => Err(EvaluateError::ValueNotFound(column.to_string()).into()),
            }
            .map(Evaluated::from)
        }
        Expr::Subquery(query) => {
            let storage =
                storage.ok_or_else(|| EvaluateError::UnsupportedStatelessExpr(expr.clone()))?;

            let evaluations = select(storage, query, context.as_ref().map(Rc::clone))
                .await?
                .map(|row| {
                    let value = match row? {
                        Row::Vec { columns, values } => {
                            if columns.len() > 1 {
                                return Err(EvaluateError::MoreThanOneColumnReturned.into());
                            }
                            values
                        }
                        Row::Map(_) => {
                            return Err(EvaluateError::SchemalessProjectionForSubQuery.into());
                        }
                    }
                    .into_iter()
                    .next();

                    Ok::<_, Error>(value)
                })
                .take(2)
                .try_collect::<Vec<_>>()
                .await?;

            if evaluations.len() > 1 {
                return Err(EvaluateError::MoreThanOneRowReturned.into());
            }

            let value = evaluations
                .into_iter()
                .next()
                .flatten()
                .unwrap_or(Value::Null);

            Ok(Evaluated::from(value))
        }
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
            .and_then(|aggregated| aggregated.get(aggr.as_ref()))
        {
            Some(value) => Ok(Evaluated::from(value.clone())),
            None => Err(EvaluateError::UnreachableEmptyAggregateValue(*aggr.clone()).into()),
        },
        Expr::Function(func) => {
            let context = context.as_ref().map(Rc::clone);
            let aggregated = aggregated.as_ref().map(Rc::clone);

            evaluate_function(storage, context, aggregated, func).await
        }
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let negated = *negated;
            let target = eval(expr).await?;

            stream::iter(list)
                .then(eval)
                .try_filter(|evaluated| ready(evaluated.evaluate_eq(&target)))
                .try_next()
                .await
                .map(|v| v.is_some() ^ negated)
                .map(Value::Bool)
                .map(Evaluated::from)
        }
        Expr::InSubquery {
            expr: target_expr,
            subquery,
            negated,
        } => {
            let storage =
                storage.ok_or_else(|| EvaluateError::UnsupportedStatelessExpr(expr.clone()))?;
            let target = eval(target_expr).await?;

            select(storage, subquery, context)
                .await?
                .map(|row| {
                    let value = match row? {
                        Row::Vec { values, .. } => values,
                        Row::Map(_) => {
                            return Err(EvaluateError::SchemalessProjectionForInSubQuery.into());
                        }
                    }
                    .into_iter()
                    .next()
                    .unwrap_or(Value::Null);

                    Ok(Evaluated::from(value))
                })
                .try_filter(|evaluated| ready(evaluated.evaluate_eq(&target)))
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
        Expr::Like {
            expr,
            negated,
            pattern,
        } => {
            let target = eval(expr).await?;
            let pattern = eval(pattern).await?;
            let evaluated = target.like(pattern, true)?;

            Ok(match negated {
                true => Evaluated::from(Value::Bool(
                    evaluated.evaluate_eq(&Evaluated::Literal(Literal::Boolean(false))),
                )),
                false => evaluated,
            })
        }
        Expr::ILike {
            expr,
            negated,
            pattern,
        } => {
            let target = eval(expr).await?;
            let pattern = eval(pattern).await?;
            let evaluated = target.like(pattern, false)?;

            Ok(match negated {
                true => Evaluated::from(Value::Bool(
                    evaluated.evaluate_eq(&Evaluated::Literal(Literal::Boolean(false))),
                )),
                false => evaluated,
            })
        }
        Expr::Exists { subquery, negated } => {
            let storage =
                storage.ok_or_else(|| EvaluateError::UnsupportedStatelessExpr(expr.clone()))?;

            select(storage, subquery, context)
                .await?
                .try_next()
                .await
                .map(|v| v.is_some() ^ negated)
                .map(Value::Bool)
                .map(Evaluated::from)
        }
        Expr::IsNull(expr) => {
            let v = eval(expr).await?.is_null();

            Ok(Evaluated::from(Value::Bool(v)))
        }
        Expr::IsNotNull(expr) => {
            let v = eval(expr).await?.is_null();

            Ok(Evaluated::from(Value::Bool(!v)))
        }
        Expr::Case {
            operand,
            when_then,
            else_result,
        } => {
            let operand = match operand {
                Some(op) => eval(op).await?,
                None => Evaluated::from(Value::Bool(true)),
            };

            for (when, then) in when_then.iter() {
                let when = eval(when).await?;

                if when.evaluate_eq(&operand) {
                    return eval(then).await;
                }
            }

            match else_result {
                Some(er) => eval(er).await,
                None => Ok(Evaluated::from(Value::Null)),
            }
        }
        Expr::ArrayIndex { obj, indexes } => {
            let obj = eval(obj).await?;
            let indexes = try_join_all(indexes.iter().map(eval)).await?;
            expr::array_index(obj, indexes)
        }
        Expr::Interval {
            expr,
            leading_field,
            last_field,
        } => {
            let value = eval(expr)
                .await
                .and_then(Value::try_from)
                .map(String::from)?;

            Interval::try_from_str(&value, *leading_field, *last_field)
                .map(Value::Interval)
                .map(Evaluated::from)
        }
    }
}

async fn evaluate_function<'a, 'b: 'a, 'c: 'a, T: GStore>(
    storage: Option<&'a T>,
    context: Option<Rc<RowContext<'b>>>,
    aggregated: Option<Rc<HashMap<&'c Aggregate, Value>>>,
    func: &'b Function,
) -> Result<Evaluated<'a>> {
    use function as f;

    let eval = |expr| {
        let context = context.as_ref().map(Rc::clone);
        let aggregated = aggregated.as_ref().map(Rc::clone);

        evaluate_inner(storage, context, aggregated, expr)
    };

    let name = func.to_string();

    match func {
        // --- text ---
        Function::Concat(exprs) => {
            let exprs = stream::iter(exprs).then(eval).try_collect().await?;
            f::concat(exprs)
        }
        Function::Custom { name, exprs } => {
            let CustomFunction {
                func_name,
                args,
                body,
            } = storage
                .ok_or(EvaluateError::UnsupportedCustomFunction)?
                .fetch_function(name)
                .await?
                .ok_or_else(|| EvaluateError::UnsupportedFunction(name.to_string()))?;

            let min = args.iter().filter(|arg| arg.default.is_none()).count();
            let max = args.len();

            if !(min..=max).contains(&exprs.len()) {
                return Err((EvaluateError::FunctionArgsLengthNotWithinRange {
                    name: func_name.to_owned(),
                    expected_minimum: min,
                    expected_maximum: max,
                    found: exprs.len(),
                })
                .into());
            }

            let exprs = exprs.iter().chain(
                args.iter()
                    .skip(exprs.len())
                    .filter_map(|arg| arg.default.as_ref()),
            );

            let context = stream::iter(args.iter().zip(exprs))
                .then(|(arg, expr)| async {
                    eval(expr)
                        .await?
                        .try_into_value(&arg.data_type, true)
                        .map(|value| (arg.name.to_owned(), value))
                })
                .try_collect()
                .await
                .map(|values| {
                    let row = Cow::Owned(Row::Map(values));
                    let context = RowContext::new(name, row, None);
                    Some(Rc::new(context))
                })?;

            evaluate_inner(storage, context, None, body).await
        }
        Function::ConcatWs { separator, exprs } => {
            let separator = eval(separator).await?;
            let exprs = stream::iter(exprs).then(eval).try_collect().await?;
            f::concat_ws(name, separator, exprs)
        }
        Function::IfNull { expr, then } => f::ifnull(eval(expr).await?, eval(then).await?),
        Function::Lower(expr) => f::lower(name, eval(expr).await?),
        Function::Initcap(expr) => f::initcap(name, eval(expr).await?),
        Function::Upper(expr) => f::upper(name, eval(expr).await?),
        Function::Left { expr, size } | Function::Right { expr, size } => {
            let expr = eval(expr).await?;
            let size = eval(size).await?;

            f::left_or_right(name, expr, size)
        }
        Function::Replace { expr, old, new } => {
            let expr = eval(expr).await?;
            let old = eval(old).await?;
            let new = eval(new).await?;
            f::replace(name, expr, old, new)
        }
        Function::Lpad { expr, size, fill } | Function::Rpad { expr, size, fill } => {
            let expr = eval(expr).await?;
            let size = eval(size).await?;
            let fill = match fill {
                Some(v) => Some(eval(v).await?),
                None => None,
            };

            f::lpad_or_rpad(name, expr, size, fill)
        }
        Function::LastDay(expr) => {
            let expr = eval(expr).await?;
            f::last_day(name, expr)
        }
        Function::Trim {
            expr,
            filter_chars,
            trim_where_field,
        } => {
            let expr = eval(expr).await?;
            let filter_chars = match filter_chars {
                Some(v) => Some(eval(v).await?),
                None => None,
            };

            expr.trim(name, filter_chars, trim_where_field)
        }
        Function::Ltrim { expr, chars } => {
            let expr = eval(expr).await?;
            let chars = match chars {
                Some(v) => Some(eval(v).await?),
                None => None,
            };

            expr.ltrim(name, chars)
        }
        Function::Rtrim { expr, chars } => {
            let expr = eval(expr).await?;
            let chars = match chars {
                Some(v) => Some(eval(v).await?),
                None => None,
            };

            expr.rtrim(name, chars)
        }
        Function::Reverse(expr) => {
            let expr = eval(expr).await?;

            f::reverse(name, expr)
        }
        Function::Repeat { expr, num } => {
            let expr = eval(expr).await?;
            let num = eval(num).await?;

            f::repeat(name, expr, num)
        }
        Function::Substr { expr, start, count } => {
            let expr = eval(expr).await?;
            let start = eval(start).await?;
            let count = match count {
                Some(v) => Some(eval(v).await?),
                None => None,
            };
            expr.substr(name, start, count)
        }
        Function::Ascii(expr) => f::ascii(name, eval(expr).await?),
        Function::Chr(expr) => f::chr(name, eval(expr).await?),
        Function::Md5(expr) => f::md5(name, eval(expr).await?),

        // --- float ---
        Function::Abs(expr) => f::abs(name, eval(expr).await?),
        Function::Sign(expr) => f::sign(name, eval(expr).await?),
        Function::Sqrt(expr) => f::sqrt(eval(expr).await?),
        Function::Power { expr, power } => {
            let expr = eval(expr).await?;
            let power = eval(power).await?;

            f::power(name, expr, power)
        }
        Function::Ceil(expr) => f::ceil(name, eval(expr).await?),
        Function::Rand(expr) => {
            let expr = match expr {
                Some(v) => Some(eval(v).await?),
                None => None,
            };
            f::rand(name, expr)
        }
        Function::Round(expr) => f::round(name, eval(expr).await?),
        Function::Floor(expr) => f::floor(name, eval(expr).await?),
        Function::Radians(expr) => f::radians(name, eval(expr).await?),
        Function::Degrees(expr) => f::degrees(name, eval(expr).await?),
        Function::Pi() => Ok(Evaluated::from(Value::F64(std::f64::consts::PI))),
        Function::Exp(expr) => f::exp(name, eval(expr).await?),
        Function::Log { antilog, base } => {
            let antilog = eval(antilog).await?;
            let base = eval(base).await?;

            f::log(name, antilog, base)
        }
        Function::Ln(expr) => f::ln(name, eval(expr).await?),
        Function::Log2(expr) => f::log2(name, eval(expr).await?),
        Function::Log10(expr) => f::log10(name, eval(expr).await?),
        Function::Sin(expr) => f::sin(name, eval(expr).await?),
        Function::Cos(expr) => f::cos(name, eval(expr).await?),
        Function::Tan(expr) => f::tan(name, eval(expr).await?),
        Function::Asin(expr) => f::asin(name, eval(expr).await?),
        Function::Acos(expr) => f::acos(name, eval(expr).await?),
        Function::Atan(expr) => f::atan(name, eval(expr).await?),

        // --- integer ---
        Function::Div { dividend, divisor } => {
            let dividend = eval(dividend).await?;
            let divisor = eval(divisor).await?;

            f::div(name, dividend, divisor)
        }
        Function::Mod { dividend, divisor } => {
            let dividend = eval(dividend).await?;
            let divisor = eval(divisor).await?;

            dividend.modulo(&divisor)
        }
        Function::Gcd { left, right } => {
            let left = eval(left).await?;
            let right = eval(right).await?;

            f::gcd(name, left, right)
        }
        Function::Lcm { left, right } => {
            let left = eval(left).await?;
            let right = eval(right).await?;

            f::lcm(name, left, right)
        }

        // --- spatial ---
        Function::Point { x, y } => {
            let x = eval(x).await?;
            let y = eval(y).await?;

            f::point(x, y)
        }
        Function::GetX(expr) => f::get_x(name, eval(expr).await?),
        Function::GetY(expr) => f::get_y(name, eval(expr).await?),
        Function::CalcDistance {
            geometry1,
            geometry2,
        } => {
            let geometry1 = eval(geometry1).await?;
            let geometry2 = eval(geometry2).await?;

            f::calc_distance(geometry1, geometry2)
        }

        // --- etc ---
        Function::Unwrap { expr, selector } => {
            let expr = eval(expr).await?;
            let selector = eval(selector).await?;

            f::unwrap(name, expr, selector)
        }
        Function::GenerateUuid() => Ok(f::generate_uuid()),
        Function::Greatest(exprs) => {
            let exprs = stream::iter(exprs).then(eval).try_collect().await?;
            f::greatest(name, exprs)
        }
        Function::Now() => Ok(Evaluated::from(Value::Timestamp(Utc::now().naive_utc()))),
        Function::Format { expr, format } => {
            let expr = eval(expr).await?;
            let format = eval(format).await?;

            f::format(name, expr, format)
        }
        Function::ToDate { expr, format } => {
            let expr = eval(expr).await?;
            let format = eval(format).await?;
            f::to_date(name, expr, format)
        }
        Function::ToTimestamp { expr, format } => {
            let expr = eval(expr).await?;
            let format = eval(format).await?;
            f::to_timestamp(name, expr, format)
        }
        Function::ToTime { expr, format } => {
            let expr = eval(expr).await?;
            let format = eval(format).await?;
            f::to_time(name, expr, format)
        }
        Function::Position {
            from_expr,
            sub_expr,
        } => {
            let from_expr = eval(from_expr).await?;
            let sub_expr = eval(sub_expr).await?;
            f::position(from_expr, sub_expr)
        }
        Function::FindIdx {
            from_expr,
            sub_expr,
            start,
        } => {
            let from_expr = eval(from_expr).await?;
            let sub_expr = eval(sub_expr).await?;
            let start = match start {
                Some(idx) => Some(eval(idx).await?),
                None => None,
            };
            f::find_idx(name, from_expr, sub_expr, start)
        }
        Function::Cast { expr, data_type } => {
            let expr = eval(expr).await?;
            f::cast(expr, data_type)
        }
        Function::Extract { field, expr } => {
            let expr = eval(expr).await?;
            f::extract(field, expr)
        }
        Function::Coalesce(exprs) => {
            let exprs = stream::iter(exprs).then(eval).try_collect().await?;
            f::coalesce(exprs)
        }

        // --- list ---
        Function::Append { expr, value } => {
            let expr = eval(expr).await?;
            let value = eval(value).await?;
            f::append(expr, value)
        }
        Function::Prepend { expr, value } => {
            let expr = eval(expr).await?;
            let value = eval(value).await?;
            f::prepend(expr, value)
        }
        Function::Skip { expr, size } => {
            let expr = eval(expr).await?;
            let size = eval(size).await?;
            f::skip(name, expr, size)
        }
        Function::Sort { expr, order } => {
            let expr = eval(expr).await?;
            let order = match order {
                Some(o) => eval(o).await?,
                None => Evaluated::from(Value::Str("ASC".to_owned())),
            };
            f::sort(expr, order)
        }
        Function::Take { expr, size } => {
            let expr = eval(expr).await?;
            let size = eval(size).await?;
            f::take(name, expr, size)
        }
        Function::Slice {
            expr,
            start,
            length,
        } => {
            let expr = eval(expr).await?;
            let start = eval(start).await?;
            let length = eval(length).await?;
            f::slice(name, expr, start, length)
        }
        Function::IsEmpty(expr) => {
            let expr = eval(expr).await?;
            f::is_empty(expr)
        }
        Function::AddMonth { expr, size } => {
            let expr = eval(expr).await?;
            let size = eval(size).await?;
            f::add_month(name, expr, size)
        }
        Function::Length(expr) => f::length(name, eval(expr).await?),
        Function::Entries(expr) => f::entries(name, eval(expr).await?),
        Function::Values(expr) => {
            let expr = eval(expr).await?;
            f::values(expr)
        }
        Function::Splice {
            list_data,
            begin_index,
            end_index,
            values,
        } => {
            let list_data = eval(list_data).await?;
            let begin_index = eval(begin_index).await?;
            let end_index = eval(end_index).await?;
            let values = match values {
                Some(v) => Some(eval(v).await?),
                None => None,
            };
            f::splice(name, list_data, begin_index, end_index, values)
        }
    }
}
