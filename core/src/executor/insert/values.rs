use {
    crate::{
        data::{Key, Row, Value},
        executor::evaluate::evaluate_stateless,
        plan::{
            ExprPlan, LimitInputPlan, LimitPlan, OffsetInputPlan, OffsetPlan, OrderByExprPlan,
            QueryPlan, ValuesOrderByPlan, ValuesPlan,
        },
        result::Result,
    },
    std::{cmp::Ordering, rc::Rc},
};

pub(super) type EvaluatedRows<'a> = Box<dyn Iterator<Item = Result<Vec<Value>>> + 'a>;
pub(super) type Rows<'a> = Box<dyn Iterator<Item = Result<Row>> + 'a>;

pub(super) fn execute<'a, F>(query: &'a QueryPlan, values: F) -> Result<Option<Rows<'a>>>
where
    F: FnOnce(&'a ValuesPlan) -> EvaluatedRows<'a>,
{
    match query {
        QueryPlan::Project(_) | QueryPlan::SelectOrderBy(_) | QueryPlan::Distinct(_) => Ok(None),
        QueryPlan::Values(plan) => Ok(Some(rows(values(plan)))),
        QueryPlan::ValuesOrderBy(plan) => Ok(Some(execute_order_by(plan, values)?)),
        QueryPlan::Offset(plan) => execute_offset(plan, values),
        QueryPlan::Limit(plan) => execute_limit(plan, values),
    }
}

fn rows(values: EvaluatedRows<'_>) -> Rows<'_> {
    Box::new(values.map(|values| values.map(row)))
}

fn row(values: Vec<Value>) -> Row {
    let columns = Rc::from(
        (1..=values.len())
            .map(|index| format!("column{index}"))
            .collect::<Vec<_>>(),
    );

    Row { columns, values }
}

fn execute_order_by<'a, F>(plan: &'a ValuesOrderByPlan, values: F) -> Result<Rows<'a>>
where
    F: FnOnce(&'a ValuesPlan) -> EvaluatedRows<'a>,
{
    let rows = rows(values(&plan.input)).collect::<Result<Vec<_>>>()?;
    let rows = sort(rows, &plan.exprs)?;

    Ok(Box::new(rows.into_iter().map(Ok)))
}

fn sort(rows: Vec<Row>, order_by: &[OrderByExprPlan]) -> Result<Vec<Row>> {
    let mut keyed_rows = Vec::with_capacity(rows.len());
    for row in rows {
        let keys = order_by
            .iter()
            .map(|OrderByExprPlan { expr, asc }| {
                evaluate_stateless(Some(row.as_context()), expr)
                    .and_then(Value::try_from)
                    .and_then(Key::try_from)
                    .map(|key| (key, *asc))
            })
            .collect::<Result<Vec<_>>>()?;

        keyed_rows.push((keys, row));
    }

    keyed_rows.sort_by(|(keys_a, _), (keys_b, _)| compare_keys(keys_a, keys_b));

    Ok(keyed_rows.into_iter().map(|(_, row)| row).collect())
}

fn compare_keys(keys_a: &[(Key, Option<bool>)], keys_b: &[(Key, Option<bool>)]) -> Ordering {
    let pairs = keys_a
        .iter()
        .map(|(a, _)| a)
        .zip(keys_b)
        .map(|(a, (b, asc))| (a, b, asc.unwrap_or(true)));

    for (key_a, key_b, asc) in pairs {
        match (key_a.cmp(key_b), asc) {
            (Ordering::Equal, _) => {}
            (ordering, true) => return ordering,
            (ordering, false) => return ordering.reverse(),
        }
    }

    Ordering::Equal
}

fn execute_offset<'a, F>(plan: &'a OffsetPlan, values: F) -> Result<Option<Rows<'a>>>
where
    F: FnOnce(&'a ValuesPlan) -> EvaluatedRows<'a>,
{
    let rows = match &plan.input {
        OffsetInputPlan::Project(_)
        | OffsetInputPlan::SelectOrderBy(_)
        | OffsetInputPlan::Distinct(_) => return Ok(None),
        OffsetInputPlan::Values(plan) => rows(values(plan)),
        OffsetInputPlan::ValuesOrderBy(plan) => execute_order_by(plan, values)?,
    };
    let count = evaluate_count(&plan.count)?;

    Ok(Some(Box::new(rows.skip(count))))
}

fn execute_limit<'a, F>(plan: &'a LimitPlan, values: F) -> Result<Option<Rows<'a>>>
where
    F: FnOnce(&'a ValuesPlan) -> EvaluatedRows<'a>,
{
    let rows = match &plan.input {
        LimitInputPlan::Project(_)
        | LimitInputPlan::SelectOrderBy(_)
        | LimitInputPlan::Distinct(_) => return Ok(None),
        LimitInputPlan::Values(plan) => rows(values(plan)),
        LimitInputPlan::ValuesOrderBy(plan) => execute_order_by(plan, values)?,
        LimitInputPlan::Offset(plan) => {
            let Some(rows) = execute_offset(plan, values)? else {
                return Ok(None);
            };

            rows
        }
    };
    let count = evaluate_count(&plan.count)?;

    Ok(Some(Box::new(rows.take(count))))
}

fn evaluate_count(expr: &ExprPlan) -> Result<usize> {
    let evaluated = evaluate_stateless(None, expr)?;
    let size: usize = Value::try_from(evaluated)?.try_into()?;

    Ok(size)
}
