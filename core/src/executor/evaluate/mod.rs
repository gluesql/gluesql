mod error;
mod evaluated;
mod expr;
mod function;
mod stateless;

use {
    super::{context::FilterContext, select::select},
    crate::{
        ast::{Aggregate, Expr, Function},
        data::Value,
        result::Result,
        store::GStore,
    },
    async_recursion::async_recursion,
    chrono::prelude::Utc,
    futures::{
        future::ready,
        stream::{self, StreamExt, TryStreamExt},
    },
    im_rc::HashMap,
    std::{borrow::Cow, rc::Rc},
};

pub use {error::EvaluateError, evaluated::Evaluated, stateless::evaluate_stateless};

#[async_recursion(?Send)]
pub async fn evalfn<'a, T>(
    expr: &'a Expr,
    storage: &'a dyn GStore<T>,
    context: Option<Rc<FilterContext<'a>>>,
    aggregated: Option<Rc<HashMap<&'a Aggregate, Value>>>,
) -> Result<Evaluated<'a>> {
    evaluate(
        storage,
        context.as_ref().map(Rc::clone),
        aggregated.as_ref().map(Rc::clone),
        expr,
    )
    .await
}

#[async_recursion(?Send)]
pub async fn evaluate<'a, T>(
    storage: &'a dyn GStore<T>,
    context: Option<Rc<FilterContext<'a>>>,
    aggregated: Option<Rc<HashMap<&'a Aggregate, Value>>>,
    expr: &'a Expr,
) -> Result<Evaluated<'a>> {
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
        Expr::Nested(expr) => evalfn(expr, storage, context, aggregated).await,
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
        Expr::Subquery(query) => {
            let evaluations = select(storage, query, context.as_ref().map(Rc::clone))
                .await?
                .map_ok(|row| row.take_first_value().map(Evaluated::from))
                .take(2)
                .try_collect::<Vec<_>>()
                .await?;

            if evaluations.len() > 1 {
                return Err(EvaluateError::MoreThanOneRowReturned.into());
            }

            evaluations
                .into_iter()
                .next()
                .unwrap_or_else(|| Err(EvaluateError::NestedSelectRowNotFound.into()))
        }
        Expr::BinaryOp { op, left, right } => {
            let left = evalfn(left, storage, context.clone(), aggregated.clone()).await?;
            let right = evalfn(right, storage, context.clone(), aggregated.clone()).await?;

            expr::binary_op(op, left, right)
        }
        Expr::UnaryOp { op, expr } => {
            let v = evalfn(expr, storage, context, aggregated).await?; //{
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
        Expr::Cast { expr, data_type } => evalfn(expr, storage, context, aggregated)
            .await?
            .cast(data_type),
        Expr::Extract { field, expr } => evalfn(expr, storage, context, aggregated)
            .await?
            .extract(field),

        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let negated = *negated;
            let target = evalfn(expr, storage, context.clone(), aggregated.clone()).await?;

            stream::iter(list)
                .then(|x| evalfn(x, storage, context.clone(), aggregated.clone()))
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
            let target = evalfn(expr, storage, context.clone(), aggregated).await?;

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
            let target = evalfn(expr, storage, context.clone(), aggregated.clone()).await?;
            let low = evalfn(low, storage, context.clone(), aggregated.clone()).await?;
            let high = evalfn(high, storage, context.clone(), aggregated.clone()).await?;

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
            let v = evalfn(expr, storage, context, aggregated).await?.is_null();
            Ok(Evaluated::from(Value::Bool(v)))
        }
        Expr::IsNotNull(expr) => {
            let v = evalfn(expr, storage, context, aggregated).await?.is_null();
            Ok(Evaluated::from(Value::Bool(!v)))
        }
        Expr::Case {
            operand,
            when_then,
            else_result,
        } => {
            let operand = match operand {
                Some(op) => evalfn(op, storage, context.clone(), aggregated.clone()).await?,
                None => Evaluated::from(Value::Bool(true)),
            };

            for (when, then) in when_then.iter() {
                let when = evalfn(when, storage, context.clone(), aggregated.clone()).await?;

                if when.eq(&operand) {
                    return evalfn(then, storage, context.clone(), aggregated.clone()).await;
                }
            }

            match else_result {
                Some(er) => evalfn(er, storage, context.clone(), aggregated.clone()).await,
                None => Ok(Evaluated::from(Value::Null)),
            }
        }
    }
}

async fn evaluate_function<'a, T>(
    storage: &'a dyn GStore<T>,
    context: Option<Rc<FilterContext<'a>>>,
    aggregated: Option<Rc<HashMap<&'a Aggregate, Value>>>,
    func: &'a Function,
) -> Result<Evaluated<'a>> {
    use function as f;
    let name = func.to_string();

    match func {
        // --- text ---
        Function::Concat(exprs) => {
            let exprs = stream::iter(exprs)
                .then(|x| evalfn(x, storage, context.clone(), aggregated.clone()))
                .try_collect()
                .await?;
            f::concat(exprs)
        }
        Function::Lower(expr) => f::lower(name, evalfn(expr, storage, context, aggregated).await?),
        Function::Upper(expr) => f::upper(name, evalfn(expr, storage, context, aggregated).await?),
        Function::Left { expr, size } | Function::Right { expr, size } => {
            let expr = evalfn(expr, storage, context.clone(), aggregated.clone()).await?;
            let size = evalfn(size, storage, context, aggregated).await?;

            f::left_or_right(name, expr, size)
        }
        Function::Lpad { expr, size, fill } | Function::Rpad { expr, size, fill } => {
            let expr = evalfn(expr, storage, context.clone(), aggregated.clone()).await?;
            let size = evalfn(size, storage, context.clone(), aggregated.clone()).await?;
            let fill = match fill {
                Some(v) => Some(evalfn(v, storage, context, aggregated).await?),
                None => None,
            };

            f::lpad_or_rpad(name, expr, size, fill)
        }
        Function::Trim {
            expr,
            filter_chars,
            trim_where_field,
        } => {
            let expr = evalfn(expr, storage, context.clone(), aggregated.clone()).await?;
            let filter_chars = match filter_chars {
                Some(v) => Some(evalfn(v, storage, context, aggregated).await?),
                None => None,
            };

            f::trim(name, expr, filter_chars, trim_where_field)
        }
        Function::Ltrim { expr, chars } => {
            let expr = evalfn(expr, storage, context.clone(), aggregated.clone()).await?;
            let chars = match chars {
                Some(v) => Some(evalfn(v, storage, context, aggregated).await?),
                None => None,
            };

            f::ltrim(name, expr, chars)
        }
        Function::Rtrim { expr, chars } => {
            let expr = evalfn(expr, storage, context.clone(), aggregated.clone()).await?;
            let chars = match chars {
                Some(v) => Some(evalfn(v, storage, context, aggregated).await?),
                None => None,
            };

            f::rtrim(name, expr, chars)
        }
        Function::Reverse(expr) => {
            let expr = evalfn(expr, storage, context, aggregated).await?;

            f::reverse(name, expr)
        }
        Function::Repeat { expr, num } => {
            let expr = evalfn(expr, storage, context.clone(), aggregated.clone()).await?;
            let num = evalfn(num, storage, context, aggregated).await?;

            f::repeat(name, expr, num)
        }
        Function::Substr { expr, start, count } => {
            let expr = evalfn(expr, storage, context.clone(), aggregated.clone()).await?;
            let start = evalfn(start, storage, context.clone(), aggregated.clone()).await?;
            let count = match count {
                Some(v) => Some(evalfn(v, storage, context, aggregated).await?),
                None => None,
            };

            f::substr(name, expr, start, count)
        }

        // --- float ---
        Function::Abs(expr) => f::abs(name, evalfn(expr, storage, context, aggregated).await?),
        Function::Sign(expr) => f::sign(name, evalfn(expr, storage, context, aggregated).await?),
        Function::Sqrt(expr) => f::sqrt(name, evalfn(expr, storage, context, aggregated).await?),
        Function::Power { expr, power } => {
            let expr = evalfn(expr, storage, context.clone(), aggregated.clone()).await?;
            let power = evalfn(power, storage, context, aggregated).await?;

            f::power(name, expr, power)
        }
        Function::Ceil(expr) => f::ceil(name, evalfn(expr, storage, context, aggregated).await?),
        Function::Round(expr) => f::round(name, evalfn(expr, storage, context, aggregated).await?),
        Function::Floor(expr) => f::floor(name, evalfn(expr, storage, context, aggregated).await?),
        Function::Radians(expr) => {
            f::radians(name, evalfn(expr, storage, context, aggregated).await?)
        }
        Function::Degrees(expr) => {
            f::degrees(name, evalfn(expr, storage, context, aggregated).await?)
        }
        Function::Pi() => Ok(Value::F64(std::f64::consts::PI)),
        Function::Exp(expr) => f::exp(name, evalfn(expr, storage, context, aggregated).await?),
        Function::Log { antilog, base } => {
            let antilog = evalfn(antilog, storage, context.clone(), aggregated.clone()).await?;
            let base = evalfn(base, storage, context, aggregated).await?;

            f::log(name, antilog, base)
        }
        Function::Ln(expr) => f::ln(name, evalfn(expr, storage, context, aggregated).await?),
        Function::Log2(expr) => f::log2(name, evalfn(expr, storage, context, aggregated).await?),
        Function::Log10(expr) => f::log10(name, evalfn(expr, storage, context, aggregated).await?),
        Function::Sin(expr) => f::sin(name, evalfn(expr, storage, context, aggregated).await?),
        Function::Cos(expr) => f::cos(name, evalfn(expr, storage, context, aggregated).await?),
        Function::Tan(expr) => f::tan(name, evalfn(expr, storage, context, aggregated).await?),
        Function::Asin(expr) => f::asin(name, evalfn(expr, storage, context, aggregated).await?),
        Function::Acos(expr) => f::acos(name, evalfn(expr, storage, context, aggregated).await?),
        Function::Atan(expr) => f::atan(name, evalfn(expr, storage, context, aggregated).await?),

        // --- integer ---
        Function::Div { dividend, divisor } => {
            let dividend = evalfn(dividend, storage, context.clone(), aggregated.clone()).await?;
            let divisor = evalfn(divisor, storage, context.clone(), aggregated.clone()).await?;

            f::div(name, dividend, divisor)
        }
        Function::Mod { dividend, divisor } => {
            let dividend = evalfn(dividend, storage, context.clone(), aggregated.clone()).await?;
            let divisor = evalfn(divisor, storage, context, aggregated).await?;

            return dividend.modulo(&divisor);
        }
        Function::Gcd { left, right } => {
            let left = evalfn(left, storage, context.clone(), aggregated.clone()).await?;
            let right = evalfn(right, storage, context, aggregated).await?;

            f::gcd(name, left, right)
        }
        Function::Lcm { left, right } => {
            let left = evalfn(left, storage, context.clone(), aggregated.clone()).await?;
            let right = evalfn(right, storage, context, aggregated).await?;

            f::lcm(name, left, right)
        }

        // --- etc ---
        Function::Unwrap { expr, selector } => {
            let expr = evalfn(expr, storage, context.clone(), aggregated.clone()).await?;
            let selector = evalfn(selector, storage, context, aggregated).await?;

            f::unwrap(name, expr, selector)
        }
        Function::GenerateUuid() => Ok(f::generate_uuid()),
        Function::Now() => Ok(Value::Timestamp(Utc::now().naive_utc())),
    }
    .map(Evaluated::from)
}
