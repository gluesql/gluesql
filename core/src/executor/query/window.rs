use {
    super::{
        SelectedRows, SelectedSources,
        aggregation::accumulator::{AggrValue, empty_value},
        filter,
        join::{inner, left_outer},
        order_by::sort_by,
        source,
    },
    crate::{
        data::{Key, Value},
        executor::{
            context::{AggregateContext, RowContext, WindowValues},
            evaluate::{EvaluateError, evaluate},
        },
        plan::{
            AggregateExprPlan, AggregateFunctionPlan, CountArgExprPlan, ExprPlan, OrderByExprPlan,
            WindowExprPlan, WindowFunctionPlan, WindowInputPlan, WindowPlan,
        },
        result::Result,
        store::GStore,
    },
    std::{collections::HashMap, rc::Rc},
};

pub(super) struct WindowedRows<'a> {
    pub(super) sources: SelectedSources<'a>,
    pub(super) rows: Vec<(AggregateContext<'a>, Option<Rc<WindowValues>>)>,
}

/// Computes window function results after the input stage (and any bare
/// aggregate) has produced its rows, but before projection.
///
/// Rows are materialized so every window slot can see the whole partition,
/// but the output preserves the original input row order: windowing never
/// reorders rows, only a query-level `ORDER BY` does.
pub(super) fn execute<'a, T>(
    storage: &'a T,
    plan: &'a WindowPlan,
    filter_context: Option<&Rc<RowContext<'a>>>,
) -> Result<WindowedRows<'a>>
where
    T: GStore,
{
    let WindowPlan {
        input,
        window_slots,
    } = plan;

    let wrap = |rows: Box<dyn Iterator<Item = Result<Rc<RowContext<'a>>>> + 'a>| {
        rows.map(|context| {
            context.map(|context| AggregateContext {
                aggregated: None,
                next: Some(context),
            })
        })
        .collect::<Result<Vec<_>>>()
    };

    let (sources, rows): (SelectedSources<'a>, Vec<AggregateContext<'a>>) = match input {
        WindowInputPlan::Source(source) => {
            let SelectedRows { sources, rows } = source::execute(storage, source)?
                .rows(None)?
                .into_selected(None);

            (sources, wrap(rows)?)
        }
        WindowInputPlan::InnerJoin(join) => {
            let SelectedRows { sources, rows } = inner::execute(storage, join, filter_context)?;

            (sources, wrap(rows)?)
        }
        WindowInputPlan::LeftOuterJoin(join) => {
            let SelectedRows { sources, rows } =
                left_outer::execute(storage, join, filter_context)?;

            (sources, wrap(rows)?)
        }
        WindowInputPlan::Filter(filter) => {
            let SelectedRows { sources, rows } = filter::execute(storage, filter, filter_context)?;

            (sources, wrap(rows)?)
        }
    };

    if window_slots.is_empty() {
        let rows = rows.into_iter().map(|context| (context, None)).collect();

        return Ok(WindowedRows { sources, rows });
    }

    let row_contexts = rows
        .iter()
        .map(|context| match (&context.next, filter_context) {
            (Some(next), Some(filter_context)) => Some(Rc::new(RowContext::concat(
                Rc::clone(next),
                Rc::clone(filter_context),
            ))),
            (Some(next), None) => Some(Rc::clone(next)),
            (None, Some(filter_context)) => Some(Rc::clone(filter_context)),
            (None, None) => None,
        })
        .collect::<Vec<_>>();

    let slot_values = compute_slots(storage, window_slots, &row_contexts)?;

    let mut slot_values_by_row = vec![Vec::with_capacity(window_slots.len()); row_contexts.len()];
    for column in slot_values {
        for (row_index, value) in column.into_iter().enumerate() {
            slot_values_by_row[row_index].push(value);
        }
    }

    let rows = rows
        .into_iter()
        .zip(slot_values_by_row)
        .map(|(context, values)| (context, Some(Rc::new(WindowValues::new(values)))))
        .collect();

    Ok(WindowedRows { sources, rows })
}

/// Computes every window slot's per-row results, grouping slots that share
/// an identical `(PARTITION BY, ORDER BY)` spec so the partition map, sort
/// order, and peer groups for that spec are computed once and reused across
/// every slot that needs them (e.g. `ROW_NUMBER() OVER w, RANK() OVER w,
/// SUM(x) OVER w`), instead of once per slot.
///
/// Returns one `Vec<Value>` per slot, indexed by row.
fn compute_slots<'a, T: GStore>(
    storage: &'a T,
    window_slots: &[WindowExprPlan],
    row_contexts: &[Option<Rc<RowContext<'a>>>],
) -> Result<Vec<Vec<Value>>> {
    let mut results = vec![vec![Value::Null; row_contexts.len()]; window_slots.len()];

    let mut spec_groups: HashMap<(&[ExprPlan], &[OrderByExprPlan]), Vec<usize>> = HashMap::new();
    for (slot, window) in window_slots.iter().enumerate() {
        let key = (
            window.over.partition_by.as_slice(),
            window.over.order_by.as_slice(),
        );
        spec_groups.entry(key).or_default().push(slot);
    }

    for slots in spec_groups.into_values() {
        let representative = &window_slots[slots[0]];
        let has_order_by = !representative.over.order_by.is_empty();

        let mut partitions: HashMap<Vec<Value>, Vec<usize>> = HashMap::new();
        for (row_index, context) in row_contexts.iter().enumerate() {
            let key = representative
                .over
                .partition_by
                .iter()
                .map(|expr| evaluate(storage, context.as_ref(), None, None, expr)?.try_into())
                .collect::<Result<Vec<Value>>>()?;

            partitions.entry(key).or_default().push(row_index);
        }

        for indices in partitions.into_values() {
            let (ordered, peer_group) = order_partition(
                storage,
                representative,
                row_contexts,
                &indices,
                has_order_by,
            )?;

            for &slot in &slots {
                compute_slot(
                    storage,
                    &window_slots[slot],
                    row_contexts,
                    &indices,
                    &ordered,
                    &peer_group,
                    has_order_by,
                    &mut results[slot],
                )?;
            }
        }
    }

    Ok(results)
}

type OrderedRow = (usize, Vec<(Key, Option<bool>)>);

/// Sorts a partition's row indices by the shared `ORDER BY` spec (or leaves
/// them unordered when there is none) and computes each row's peer-group
/// number. A peer group is a maximal run of consecutive rows (in window
/// order) that are equal on every `ORDER BY` expression; with no `ORDER BY`,
/// the whole partition is a single peer group.
fn order_partition<'a, T: GStore>(
    storage: &'a T,
    window: &WindowExprPlan,
    row_contexts: &[Option<Rc<RowContext<'a>>>],
    indices: &[usize],
    has_order_by: bool,
) -> Result<(Vec<OrderedRow>, Vec<usize>)> {
    let ordered = if has_order_by {
        let mut paired = indices
            .iter()
            .map(|&row_index| {
                let context = row_contexts[row_index].as_ref();
                let keys = window
                    .over
                    .order_by
                    .iter()
                    .map(|OrderByExprPlan { expr, asc }| {
                        let value: Value =
                            evaluate(storage, context, None, None, expr)?.try_into()?;

                        Key::try_from(value).map(|key| (key, *asc))
                    })
                    .collect::<Result<Vec<_>>>()?;

                Ok((row_index, keys))
            })
            .collect::<Result<Vec<_>>>()?;

        paired.sort_by(|(_, a), (_, b)| sort_by(a, b));

        paired
    } else {
        indices
            .iter()
            .map(|&row_index| (row_index, Vec::new()))
            .collect()
    };

    let mut peer_group = Vec::with_capacity(ordered.len());
    let mut current_group = 0;

    for position in 0..ordered.len() {
        if position > 0 {
            let (_, keys) = &ordered[position];
            let (_, previous_keys) = &ordered[position - 1];
            let same = keys
                .iter()
                .map(|(key, _)| key)
                .eq(previous_keys.iter().map(|(key, _)| key));

            if !same {
                current_group += 1;
            }
        }

        peer_group.push(current_group);
    }

    Ok((ordered, peer_group))
}

#[allow(clippy::too_many_arguments)]
fn compute_slot<'a, T: GStore>(
    storage: &'a T,
    window: &WindowExprPlan,
    row_contexts: &[Option<Rc<RowContext<'a>>>],
    indices: &[usize],
    ordered: &[OrderedRow],
    peer_group: &[usize],
    has_order_by: bool,
    result: &mut [Value],
) -> Result<()> {
    let row_at = |position: usize| ordered[position].0;

    match &window.func {
        WindowFunctionPlan::RowNumber => {
            for (position, &(row_index, _)) in ordered.iter().enumerate() {
                result[row_index] = Value::I64((position + 1) as i64);
            }
        }
        WindowFunctionPlan::Rank => {
            let mut first_position_of_group: HashMap<usize, usize> = HashMap::new();

            for (position, &group) in peer_group.iter().enumerate() {
                first_position_of_group.entry(group).or_insert(position);
            }

            for (position, &(row_index, _)) in ordered.iter().enumerate() {
                let group = peer_group[position];
                result[row_index] = Value::I64((first_position_of_group[&group] + 1) as i64);
            }
        }
        WindowFunctionPlan::DenseRank => {
            for (position, &(row_index, _)) in ordered.iter().enumerate() {
                result[row_index] = Value::I64((peer_group[position] + 1) as i64);
            }
        }
        WindowFunctionPlan::Lag {
            expr,
            offset,
            default,
        }
        | WindowFunctionPlan::Lead {
            expr,
            offset,
            default,
        } => {
            let is_lag = matches!(window.func, WindowFunctionPlan::Lag { .. });

            for position in 0..ordered.len() {
                let row_index = row_at(position);
                let context = row_contexts[row_index].as_ref();
                let offset_value: Value =
                    evaluate(storage, context, None, None, offset)?.try_into()?;
                let offset_value = match offset_value {
                    Value::I64(n) => match usize::try_from(n) {
                        Ok(offset_value) => offset_value,
                        Err(_) => return Err(EvaluateError::WindowOffsetOutOfRange(n).into()),
                    },
                    _ => return Err(EvaluateError::WindowOffsetNotInteger.into()),
                };

                let target = if is_lag {
                    position.checked_sub(offset_value)
                } else {
                    position
                        .checked_add(offset_value)
                        .filter(|&target| target < ordered.len())
                };

                result[row_index] = match target {
                    Some(target) => {
                        let target_context = row_contexts[row_at(target)].as_ref();
                        evaluate(storage, target_context, None, None, expr)?.try_into()?
                    }
                    None => match default {
                        Some(default) => {
                            evaluate(storage, context, None, None, default)?.try_into()?
                        }
                        None => Value::Null,
                    },
                };
            }
        }
        WindowFunctionPlan::Aggregate(aggregate) => {
            if has_order_by {
                let mut position = 0;
                let mut accumulator: Option<AggrValue> = None;

                while position < ordered.len() {
                    let group = peer_group[position];
                    let group_end = ordered.len().min(
                        position
                            + peer_group[position..]
                                .iter()
                                .take_while(|&&g| g == group)
                                .count(),
                    );

                    for row_index in (position..group_end).map(row_at) {
                        let context = row_contexts[row_index].as_ref();
                        let value = aggregate_input(storage, aggregate, context)?;

                        if AggrValue::ignores(aggregate, &value) {
                            continue;
                        }

                        match accumulator.as_mut() {
                            Some(accumulator) => {
                                accumulator.accumulate(&value)?;
                            }
                            None => accumulator = Some(AggrValue::new(aggregate, &value)?),
                        }
                    }

                    let exported = match &accumulator {
                        Some(accumulator) => accumulator.clone().export()?,
                        None => empty_value(aggregate),
                    };

                    for row_index in (position..group_end).map(row_at) {
                        result[row_index] = exported.clone();
                    }

                    position = group_end;
                }
            } else {
                let mut accumulator: Option<AggrValue> = None;

                for &row_index in indices {
                    let context = row_contexts[row_index].as_ref();
                    let value = aggregate_input(storage, aggregate, context)?;

                    if AggrValue::ignores(aggregate, &value) {
                        continue;
                    }

                    match accumulator.as_mut() {
                        Some(accumulator) => {
                            accumulator.accumulate(&value)?;
                        }
                        None => accumulator = Some(AggrValue::new(aggregate, &value)?),
                    }
                }

                let exported = match accumulator {
                    Some(accumulator) => accumulator.export()?,
                    None => empty_value(aggregate),
                };

                for &row_index in indices {
                    result[row_index] = exported.clone();
                }
            }
        }
    }

    Ok(())
}

fn aggregate_input<'a, T: GStore>(
    storage: &'a T,
    aggregate: &AggregateExprPlan,
    context: Option<&Rc<RowContext<'a>>>,
) -> Result<Value> {
    match &aggregate.func {
        AggregateFunctionPlan::Count(CountArgExprPlan::Wildcard) => Ok(Value::Null),
        AggregateFunctionPlan::Count(CountArgExprPlan::Expr(expr))
        | AggregateFunctionPlan::Sum(expr)
        | AggregateFunctionPlan::Min(expr)
        | AggregateFunctionPlan::Max(expr)
        | AggregateFunctionPlan::Avg(expr)
        | AggregateFunctionPlan::Variance(expr)
        | AggregateFunctionPlan::Stdev(expr) => {
            evaluate(storage, context, None, None, expr)?.try_into()
        }
    }
}
