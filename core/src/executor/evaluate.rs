mod error;
mod evaluated;
mod expr;
mod function;

use {
    self::function::BreakCase,
    super::{
        context::{AggregateValues, RowContext},
        select::{select, select_with_labels},
    },
    crate::{
        data::{CustomFunction, Interval, Row, Value},
        mock::MockStorage,
        plan::{ExprPlan, FunctionPlan, ProjectionPlan, SetExprPlan, plan_scalar_expr},
        result::{Error, Result},
        store::GStore,
    },
    chrono::prelude::Utc,
    std::{borrow::Cow, ops::ControlFlow, rc::Rc},
};

pub use {error::EvaluateError, evaluated::Evaluated};

pub fn evaluate<'a, 'b, T>(
    storage: &'a T,
    context: Option<&Rc<RowContext<'b>>>,
    aggregated: Option<&Rc<AggregateValues>>,
    expr: &'a ExprPlan,
) -> Result<Evaluated<'a>>
where
    'b: 'a,
    T: GStore,
{
    evaluate_inner(Some(storage), context, aggregated, expr)
}

pub fn evaluate_stateless<'a, 'b: 'a>(
    context: Option<RowContext<'b>>,
    expr: &'a ExprPlan,
) -> Result<Evaluated<'a>> {
    let context = context.map(Rc::new);
    let storage: Option<&MockStorage> = None;

    evaluate_inner(storage, context.as_ref(), None, expr)
}

fn evaluate_inner<'a, 'b, T>(
    storage: Option<&'a T>,
    context: Option<&Rc<RowContext<'b>>>,
    aggregated: Option<&Rc<AggregateValues>>,
    expr: &'a ExprPlan,
) -> Result<Evaluated<'a>>
where
    'b: 'a,
    T: GStore,
{
    let eval = |expr| evaluate_inner(storage, context, aggregated, expr);

    match expr {
        ExprPlan::Literal(literal) => Ok(expr::literal(literal)),
        ExprPlan::Value(value) => Ok(Evaluated::Value(Cow::Borrowed(value))),
        ExprPlan::TypedString { data_type, value } => expr::typed_string(data_type, value),
        ExprPlan::Identifier(ident) => {
            let context = context
                .ok_or_else(|| EvaluateError::IdentifierRequiresRowContext(ident.to_owned()))?;

            match context.get_value(ident) {
                Some(value) => Ok(Evaluated::Value(Cow::Owned(value.clone()))),
                None => Err(EvaluateError::IdentifierNotFound(ident.to_owned()).into()),
            }
        }
        ExprPlan::Nested(expr) => eval(expr),
        ExprPlan::CompoundIdentifier { alias, ident } => {
            let context =
                context.ok_or_else(|| EvaluateError::CompoundIdentifierRequiresRowContext {
                    alias: alias.to_owned(),
                    ident: ident.to_owned(),
                })?;

            match context.get_alias_value(alias, ident) {
                Some(value) => Ok(Evaluated::Value(Cow::Owned(value.clone()))),
                None => Err(EvaluateError::CompoundIdentifierNotFound {
                    table_alias: alias.to_owned(),
                    column_name: ident.to_owned(),
                }
                .into()),
            }
        }
        ExprPlan::Subquery(query) => {
            let storage = storage.ok_or(EvaluateError::SubqueryNotAllowedInStatelessExpr)?;
            if let SetExprPlan::Select(select) = &query.body
                && matches!(select.projection, ProjectionPlan::SchemalessMap)
            {
                return Err(EvaluateError::SchemalessProjectionForSubQuery.into());
            }

            let evaluations = select(storage, query, context.cloned())?
                .map(|row| {
                    let values = row?.into_values();
                    if values.len() > 1 {
                        return Err(EvaluateError::MoreThanOneColumnReturned.into());
                    }
                    let value = values.into_iter().next();

                    Ok::<_, Error>(value)
                })
                .take(2)
                .collect::<Result<Vec<_>>>()?;

            if evaluations.len() > 1 {
                return Err(EvaluateError::MoreThanOneRowReturned.into());
            }

            let value = evaluations
                .into_iter()
                .next()
                .flatten()
                .unwrap_or(Value::Null);

            Ok(Evaluated::Value(Cow::Owned(value)))
        }
        ExprPlan::BinaryOp { op, left, right } => {
            let left = eval(left)?;
            let right = eval(right)?;

            expr::binary_op(op, left, right)
        }
        ExprPlan::UnaryOp { op, expr } => {
            let v = eval(expr)?;

            expr::unary_op(op, v)
        }
        ExprPlan::Aggregate(aggr) => match aggregated
            .as_ref()
            .and_then(|aggregated| aggr.slot.and_then(|slot| aggregated.get(slot)))
        {
            Some(value) => Ok(Evaluated::Value(Cow::Owned(value.clone()))),
            None if aggr.slot.is_none() => {
                Err(EvaluateError::UnplannedAggregate(aggr.clone()).into())
            }
            None => Err(EvaluateError::AggregateSlotValueMissing(aggr.clone()).into()),
        },
        ExprPlan::Function(func) => evaluate_function(storage, context, aggregated, func),
        ExprPlan::InList {
            expr,
            list,
            negated,
        } => {
            let negated = *negated;
            let target = eval(expr)?;

            if target.is_null() {
                return Ok(target);
            }

            let matched = list
                .iter()
                .map(eval)
                .collect::<Result<Vec<_>>>()?
                .into_iter()
                .any(|v| v.evaluate_eq(&target).is_true());

            Ok(Evaluated::Value(Cow::Owned(Value::Bool(matched ^ negated))))
        }
        ExprPlan::InSubquery {
            expr: target_expr,
            subquery,
            negated,
        } => {
            let storage = storage.ok_or(EvaluateError::InSubqueryNotAllowedInStatelessExpr)?;
            if let SetExprPlan::Select(select) = &subquery.body
                && matches!(select.projection, ProjectionPlan::SchemalessMap)
            {
                return Err(EvaluateError::SchemalessProjectionForInSubQuery.into());
            }
            let target = eval(target_expr)?;
            let (labels, rows) = select_with_labels(storage, subquery, context.cloned())?;

            if labels.len() > 1 {
                return Err(EvaluateError::InSubqueryMustReturnOneColumn.into());
            }

            let mut matched = false;
            for row in rows {
                let value = row?.into_values().into_iter().next().unwrap_or(Value::Null);
                let evaluated = Evaluated::Value(Cow::Owned(value));

                if evaluated.evaluate_eq(&target).is_true() {
                    matched = true;
                    break;
                }
            }

            Ok(Evaluated::Value(Cow::Owned(Value::Bool(matched ^ negated))))
        }
        ExprPlan::Between {
            expr,
            negated,
            low,
            high,
        } => {
            let target = eval(expr)?;
            let low = eval(low)?;
            let high = eval(high)?;

            Ok(expr::between(&target, *negated, &low, &high))
        }
        ExprPlan::Like {
            expr,
            negated,
            pattern,
        } => {
            let target = eval(expr)?;
            let pattern = eval(pattern)?;
            let evaluated = target.like(pattern, true)?;

            Ok(match negated {
                true => {
                    let t =
                        evaluated.evaluate_eq(&Evaluated::Value(Cow::Owned(Value::Bool(false))));
                    Evaluated::Value(Cow::Owned(Value::from(t)))
                }
                false => evaluated,
            })
        }
        ExprPlan::ILike {
            expr,
            negated,
            pattern,
        } => {
            let target = eval(expr)?;
            let pattern = eval(pattern)?;
            let evaluated = target.like(pattern, false)?;

            Ok(match negated {
                true => {
                    let t =
                        evaluated.evaluate_eq(&Evaluated::Value(Cow::Owned(Value::Bool(false))));
                    Evaluated::Value(Cow::Owned(Value::from(t)))
                }
                false => evaluated,
            })
        }
        ExprPlan::Exists { subquery, negated } => {
            let storage = storage.ok_or(EvaluateError::ExistsSubqueryNotAllowedInStatelessExpr)?;

            let exists = select(storage, subquery, context.cloned())?
                .next()
                .transpose()?
                .is_some();

            Ok(Evaluated::Value(Cow::Owned(Value::Bool(exists ^ negated))))
        }
        ExprPlan::IsNull(expr) => {
            let v = eval(expr)?.is_null();

            Ok(Evaluated::Value(Cow::Owned(Value::Bool(v))))
        }
        ExprPlan::IsNotNull(expr) => {
            let v = eval(expr)?.is_null();

            Ok(Evaluated::Value(Cow::Owned(Value::Bool(!v))))
        }
        ExprPlan::Case {
            operand,
            when_then,
            else_result,
        } => {
            let operand = match operand {
                Some(op) => eval(op)?,
                None => Evaluated::Value(Cow::Owned(Value::Bool(true))),
            };

            for (when, then) in when_then {
                let when = eval(when)?;

                if when.evaluate_eq(&operand).is_true() {
                    return eval(then);
                }
            }

            match else_result {
                Some(er) => eval(er),
                None => Ok(Evaluated::Value(Cow::Owned(Value::Null))),
            }
        }
        ExprPlan::ArrayIndex { obj, indexes } => {
            let obj = eval(obj)?;
            let indexes = indexes.iter().map(eval).collect::<Result<Vec<_>>>()?;
            expr::array_index(obj, indexes)
        }
        ExprPlan::Array { elem } => elem
            .iter()
            .map(eval)
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .map(Value::try_from)
            .collect::<Result<Vec<_>>>()
            .map(Value::List)
            .map(|v| Evaluated::Value(Cow::Owned(v))),
        ExprPlan::Interval {
            expr,
            leading_field,
            last_field,
        } => {
            let value = eval(expr).and_then(Value::try_from).map(String::from)?;

            Interval::try_from_str(&value, *leading_field, *last_field)
                .map(Value::Interval)
                .map(|v| Evaluated::Value(Cow::Owned(v)))
        }
    }
}

fn evaluate_function<'a, 'b: 'a, T: GStore>(
    storage: Option<&'a T>,
    context: Option<&Rc<RowContext<'b>>>,
    aggregated: Option<&Rc<AggregateValues>>,
    func: &'a FunctionPlan,
) -> Result<Evaluated<'a>> {
    use function as f;

    let eval = |expr| evaluate_inner(storage, context, aggregated, expr);

    let name = func.to_string();

    let result = match func {
        // --- text ---
        FunctionPlan::Concat(exprs) => {
            let exprs = exprs.iter().map(eval).collect::<Result<Vec<_>>>()?;
            f::concat(exprs)
        }
        FunctionPlan::Custom { name, exprs } => {
            let CustomFunction {
                func_name,
                args,
                body,
            } = storage
                .ok_or(EvaluateError::UnsupportedCustomFunction)?
                .fetch_function(name)?
                .ok_or_else(|| EvaluateError::UnsupportedFunction(name.clone()))?;

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

            let mut pairs = Vec::with_capacity(args.len());
            for (index, arg) in args.iter().enumerate() {
                let value = if let Some(expr) = exprs.get(index) {
                    eval(expr)?.try_into_value(&arg.data_type, true)
                } else {
                    let default = arg.default.as_ref().ok_or_else(|| {
                        EvaluateError::FunctionArgsLengthNotWithinRange {
                            name: func_name.to_owned(),
                            expected_minimum: min,
                            expected_maximum: max,
                            found: exprs.len(),
                        }
                    })?;
                    let default = plan_scalar_expr(default.clone());

                    evaluate_inner(storage, context, aggregated, &default)?
                        .try_into_value(&arg.data_type, true)
                }?;

                pairs.push((arg.name.clone(), value));
            }

            let (columns, values): (Vec<_>, Vec<_>) = pairs.into_iter().unzip();
            let row = Cow::Owned(Row {
                columns: columns.into(),
                values,
            });
            let context = RowContext::new(name, row, None);
            let context = Some(Rc::new(context));

            let body = plan_scalar_expr(body.clone());
            let evaluated = evaluate_inner(storage, context.as_ref(), None, &body)?;
            let value = evaluated.try_into()?;

            return Ok(Evaluated::Value(Cow::Owned(value)));
        }
        FunctionPlan::ConcatWs { separator, exprs } => {
            let separator = eval(separator)?;
            let exprs = exprs.iter().map(eval).collect::<Result<Vec<_>>>()?;
            f::concat_ws(&name, separator, exprs)
        }
        FunctionPlan::IfNull { expr, then } => f::ifnull(eval(expr)?, eval(then)?),
        FunctionPlan::NullIf { expr1, expr2 } => f::nullif(eval(expr1)?, &eval(expr2)?),
        FunctionPlan::Lower(expr) => f::lower(&name, eval(expr)?),
        FunctionPlan::Initcap(expr) => f::initcap(&name, eval(expr)?),
        FunctionPlan::Upper(expr) => f::upper(&name, eval(expr)?),
        FunctionPlan::Left { expr, size } | FunctionPlan::Right { expr, size } => {
            let expr = eval(expr)?;
            let size = eval(size)?;

            f::left_or_right(&name, expr, size)
        }
        FunctionPlan::Replace { expr, old, new } => {
            let expr = eval(expr)?;
            let old = eval(old)?;
            let new = eval(new)?;

            f::replace(&name, expr, old, new)
        }
        FunctionPlan::Lpad { expr, size, fill } | FunctionPlan::Rpad { expr, size, fill } => {
            let expr = eval(expr)?;
            let size = eval(size)?;
            let fill = match fill {
                Some(v) => Some(eval(v)?),
                None => None,
            };

            f::lpad_or_rpad(&name, expr, size, fill)
        }
        FunctionPlan::LastDay(expr) => {
            let expr = eval(expr)?;
            f::last_day(&name, expr)
        }
        FunctionPlan::Trim {
            expr,
            filter_chars,
            trim_where_field,
        } => {
            let expr = eval(expr)?;
            let filter_chars = match filter_chars {
                Some(v) => Some(eval(v)?),
                None => None,
            };

            return expr.trim(name, filter_chars, trim_where_field.as_ref());
        }
        FunctionPlan::Ltrim { expr, chars } => {
            let expr = eval(expr)?;
            let chars = match chars {
                Some(v) => Some(eval(v)?),
                None => None,
            };

            return expr.ltrim(name, chars);
        }
        FunctionPlan::Rtrim { expr, chars } => {
            let expr = eval(expr)?;
            let chars = match chars {
                Some(v) => Some(eval(v)?),
                None => None,
            };

            return expr.rtrim(name, chars);
        }
        FunctionPlan::Reverse(expr) => {
            let expr = eval(expr)?;

            f::reverse(&name, expr)
        }
        FunctionPlan::Repeat { expr, num } => {
            let expr = eval(expr)?;
            let num = eval(num)?;

            f::repeat(&name, expr, num)
        }
        FunctionPlan::Substr { expr, start, count } => {
            let expr = eval(expr)?;
            let start = eval(start)?;
            let count = match count {
                Some(v) => Some(eval(v)?),
                None => None,
            };

            return expr.substr(name, start, count);
        }
        FunctionPlan::Ascii(expr) => f::ascii(&name, eval(expr)?),
        FunctionPlan::Chr(expr) => f::chr(&name, eval(expr)?),
        FunctionPlan::Md5(expr) => f::md5(&name, eval(expr)?),
        FunctionPlan::Hex(expr) => f::hex(&name, eval(expr)?),

        // --- float ---
        FunctionPlan::Abs(expr) => f::abs(&name, eval(expr)?),
        FunctionPlan::Sign(expr) => f::sign(&name, eval(expr)?),
        FunctionPlan::Sqrt(expr) => f::sqrt(eval(expr)?),
        FunctionPlan::Power { expr, power } => {
            let expr = eval(expr)?;
            let power = eval(power)?;

            f::power(&name, expr, power)
        }
        FunctionPlan::Ceil(expr) => f::ceil(&name, eval(expr)?),
        FunctionPlan::Rand(expr) => {
            let expr = match expr {
                Some(v) => Some(eval(v)?),
                None => None,
            };

            f::rand(&name, expr)
        }
        FunctionPlan::Round(expr) => f::round(&name, eval(expr)?),
        FunctionPlan::Trunc(expr) => f::trunc(&name, eval(expr)?),
        FunctionPlan::Floor(expr) => f::floor(&name, eval(expr)?),
        FunctionPlan::Radians(expr) => f::radians(&name, eval(expr)?),
        FunctionPlan::Degrees(expr) => f::degrees(&name, eval(expr)?),
        FunctionPlan::Pi() => {
            return Ok(Evaluated::Value(Cow::Owned(Value::F64(
                std::f64::consts::PI,
            ))));
        }
        FunctionPlan::Exp(expr) => f::exp(&name, eval(expr)?),
        FunctionPlan::Log { antilog, base } => {
            let antilog = eval(antilog)?;
            let base = eval(base)?;

            f::log(&name, antilog, base)
        }
        FunctionPlan::Ln(expr) => f::ln(&name, eval(expr)?),
        FunctionPlan::Log2(expr) => f::log2(&name, eval(expr)?),
        FunctionPlan::Log10(expr) => f::log10(&name, eval(expr)?),
        FunctionPlan::Sin(expr) => f::sin(&name, eval(expr)?),
        FunctionPlan::Cos(expr) => f::cos(&name, eval(expr)?),
        FunctionPlan::Tan(expr) => f::tan(&name, eval(expr)?),
        FunctionPlan::Asin(expr) => f::asin(&name, eval(expr)?),
        FunctionPlan::Acos(expr) => f::acos(&name, eval(expr)?),
        FunctionPlan::Atan(expr) => f::atan(&name, eval(expr)?),

        // --- integer ---
        FunctionPlan::Div { dividend, divisor } => {
            let dividend = eval(dividend)?;
            let divisor = eval(divisor)?;

            f::div(&name, dividend, divisor)
        }
        FunctionPlan::Mod { dividend, divisor } => {
            let dividend = eval(dividend)?;
            let divisor = eval(divisor)?;

            return dividend.modulo(&divisor);
        }
        FunctionPlan::Gcd { left, right } => {
            let left = eval(left)?;
            let right = eval(right)?;

            f::gcd(&name, left, right)
        }
        FunctionPlan::Lcm { left, right } => {
            let left = eval(left)?;
            let right = eval(right)?;

            f::lcm(&name, left, right)
        }

        // --- spatial ---
        FunctionPlan::Point { x, y } => {
            let x = eval(x)?;
            let y = eval(y)?;

            f::point(&name, x, y)
        }
        FunctionPlan::GetX(expr) => f::get_x(&name, eval(expr)?),
        FunctionPlan::GetY(expr) => f::get_y(&name, eval(expr)?),
        FunctionPlan::CalcDistance {
            geometry1,
            geometry2,
        } => {
            let geometry1 = eval(geometry1)?;
            let geometry2 = eval(geometry2)?;

            f::calc_distance(&name, geometry1, geometry2)
        }

        // --- etc ---
        FunctionPlan::Unwrap { expr, selector } => {
            let expr = eval(expr)?;
            let selector = eval(selector)?;

            f::unwrap(&name, expr, selector)
        }
        FunctionPlan::GenerateUuid() => return Ok(f::generate_uuid()),
        FunctionPlan::Greatest(exprs) => {
            let exprs = exprs.iter().map(eval).collect::<Result<Vec<_>>>()?;
            return f::greatest(&name, exprs);
        }
        FunctionPlan::Now() | FunctionPlan::CurrentTimestamp() => {
            return Ok(Evaluated::Value(Cow::Owned(Value::Timestamp(
                Utc::now().naive_utc(),
            ))));
        }
        FunctionPlan::CurrentDate() => {
            return Ok(Evaluated::Value(Cow::Owned(Value::Date(
                Utc::now().date_naive(),
            ))));
        }
        FunctionPlan::CurrentTime() => {
            return Ok(Evaluated::Value(Cow::Owned(Value::Time(Utc::now().time()))));
        }
        FunctionPlan::Format { expr, format } => {
            let expr = eval(expr)?;
            let format = eval(format)?;

            f::format(&name, expr, format)
        }
        FunctionPlan::ToDate { expr, format } => {
            let expr = eval(expr)?;
            let format = eval(format)?;
            f::to_date(&name, expr, format)
        }
        FunctionPlan::ToTimestamp { expr, format } => {
            let expr = eval(expr)?;
            let format = eval(format)?;
            f::to_timestamp(&name, expr, format)
        }
        FunctionPlan::ToTime { expr, format } => {
            let expr = eval(expr)?;
            let format = eval(format)?;
            f::to_time(&name, expr, format)
        }
        FunctionPlan::Position {
            from_expr,
            sub_expr,
        } => {
            let from_expr = eval(from_expr)?;
            let sub_expr = eval(sub_expr)?;
            f::position(from_expr, sub_expr)
        }
        FunctionPlan::FindIdx {
            from_expr,
            sub_expr,
            start,
        } => {
            let from_expr = eval(from_expr)?;
            let sub_expr = eval(sub_expr)?;
            let start = match start {
                Some(idx) => Some(eval(idx)?),
                None => None,
            };
            f::find_idx(&name, from_expr, sub_expr, start)
        }
        FunctionPlan::Cast { expr, data_type } => return eval(expr)?.cast(data_type),
        FunctionPlan::Extract { field, expr } => {
            let expr = eval(expr)?;
            f::extract(*field, expr)
        }
        FunctionPlan::Coalesce(exprs) => {
            let exprs = exprs.iter().map(eval).collect::<Result<Vec<_>>>()?;
            return f::coalesce(exprs);
        }

        // --- list ---
        FunctionPlan::Append { expr, value } => {
            let expr = eval(expr)?;
            let value = eval(value)?;
            f::append(expr, value)
        }
        FunctionPlan::Prepend { expr, value } => {
            let expr = eval(expr)?;
            let value = eval(value)?;
            f::prepend(expr, value)
        }
        FunctionPlan::Skip { expr, size } => {
            let expr = eval(expr)?;
            let size = eval(size)?;
            f::skip(&name, expr, size)
        }
        FunctionPlan::Sort { expr, order } => {
            let expr = eval(expr)?;
            let order = match order {
                Some(o) => eval(o)?,
                None => Evaluated::Value(Cow::Owned(Value::Str("ASC".to_owned()))),
            };
            f::sort(expr, order)
        }
        FunctionPlan::Take { expr, size } => {
            let expr = eval(expr)?;
            let size = eval(size)?;
            f::take(&name, expr, size)
        }
        FunctionPlan::Slice {
            expr,
            start,
            length,
        } => {
            let expr = eval(expr)?;
            let start = eval(start)?;
            let length = eval(length)?;
            f::slice(&name, expr, start, length)
        }
        FunctionPlan::IsEmpty(expr) => {
            let expr = eval(expr)?;
            f::is_empty(expr)
        }
        FunctionPlan::AddMonth { expr, size } => {
            let expr = eval(expr)?;
            let size = eval(size)?;
            f::add_month(&name, expr, size)
        }
        FunctionPlan::Length(expr) => f::length(&name, eval(expr)?),
        FunctionPlan::Entries(expr) => f::entries(&name, eval(expr)?),
        FunctionPlan::Keys(expr) => f::keys(eval(expr)?),
        FunctionPlan::Values(expr) => {
            let expr = eval(expr)?;
            f::values(expr)
        }
        FunctionPlan::Splice {
            list_data,
            begin_index,
            end_index,
            values,
        } => {
            let list_data = eval(list_data)?;
            let begin_index = eval(begin_index)?;
            let end_index = eval(end_index)?;
            let values = match values {
                Some(v) => Some(eval(v)?),
                None => None,
            };
            f::splice(&name, list_data, begin_index, end_index, values)
        }
        FunctionPlan::Dedup(list) => f::dedup(eval(list)?),
    };

    match result {
        ControlFlow::Continue(v) => Ok(v),
        ControlFlow::Break(BreakCase::Null) => Ok(Evaluated::Value(Cow::Owned(Value::Null))),
        ControlFlow::Break(BreakCase::Err(err)) => Err(err),
    }
}

#[cfg(test)]
mod tests {
    use {
        super::{EvaluateError, evaluate, evaluate_stateless},
        crate::{
            ast::{Expr, Projection, SelectItem, SetExpr, Statement},
            executor::context::AggregateValues,
            mock::MockStorage,
            parse_sql::parse,
            plan::{AggregateFunctionPlan, AggregatePlan, CountArgExprPlan, ExprPlan},
            result::Error,
            translate::translate,
        },
        std::rc::Rc,
    };

    #[test]
    fn aggregate_requires_planner_binding() {
        let sql = "SELECT COUNT(*) FROM Item";
        let parsed = parse(sql)
            .expect(sql)
            .into_iter()
            .next()
            .expect("query statement");
        let translated = translate(&parsed).expect("translated statement");

        let expr = if let Statement::Query(query) = translated
            && let SetExpr::Select(select) = query.body
            && let Projection::SelectItems(items) = select.projection
            && let Some(SelectItem::Expr { expr, .. }) = items.into_iter().next()
        {
            expr
        } else {
            panic!("expected SELECT projection expression: {sql}");
        };

        let Expr::Aggregate(aggregate) = expr else {
            panic!("expected aggregate expression");
        };

        let expr = ExprPlan::Aggregate(Box::new(AggregatePlan::from((*aggregate).clone())));
        let result = evaluate_stateless(None, &expr);

        assert_eq!(
            result,
            Err(Error::from(EvaluateError::UnplannedAggregate(Box::new(
                AggregatePlan::from(*aggregate)
            ))))
        );
    }

    #[test]
    fn aggregate_slot_value_must_exist() {
        let aggregate = AggregatePlan {
            func: AggregateFunctionPlan::Count(CountArgExprPlan::Wildcard),
            distinct: false,
            slot: Some(0),
        };

        let expr = ExprPlan::Aggregate(Box::new(aggregate.clone()));
        let storage = MockStorage::default();
        let aggregated = Rc::new(AggregateValues::new(Vec::new()));

        let result = evaluate(&storage, None, Some(&aggregated), &expr);

        assert_eq!(
            result,
            Err(Error::from(EvaluateError::AggregateSlotValueMissing(
                Box::new(aggregate)
            )))
        );
    }
}
