use {
    crate::{
        ast::{Aggregate, AggregateFunction, CountArgExpr, DataType},
        data::Value,
        executor::{
            context::{AggregateContext, AggregateValues, RowContext},
            evaluate::{EvaluateError, evaluate},
        },
        result::Result,
        store::GStore,
    },
    std::{
        cmp::Ordering,
        collections::{HashMap, HashSet},
        sync::Arc,
    },
};

#[derive(Clone)]
enum AggrValue {
    Count {
        wildcard: bool,
        count: i64,
        distinct_values: Option<HashSet<Value>>,
    },
    Sum {
        value: Value,
        distinct_values: Option<HashSet<Value>>,
    },
    Min {
        value: Value,
        distinct_values: Option<HashSet<Value>>,
    },
    Max {
        value: Value,
        distinct_values: Option<HashSet<Value>>,
    },
    Avg {
        sum: Value,
        count: i64,
        distinct_values: Option<HashSet<Value>>,
    },
    Variance {
        sum_square: Value,
        sum: Value,
        count: i64,
        distinct_values: Option<HashSet<Value>>,
    },
    Stdev {
        sum_square: Value,
        sum: Value,
        count: i64,
        distinct_values: Option<HashSet<Value>>,
    },
}

impl AggrValue {
    fn track_distinct(distinct_values: &mut Option<HashSet<Value>>, new_value: &Value) -> bool {
        match distinct_values {
            Some(set) => {
                if set.contains(new_value) {
                    false
                } else {
                    set.insert(new_value.clone());
                    true
                }
            }
            None => true,
        }
    }

    fn new(aggregate: &Aggregate, value: &Value) -> Result<Self> {
        let value = value.clone();

        Ok(match &aggregate.func {
            AggregateFunction::Count(CountArgExpr::Wildcard) => {
                let distinct_values = aggregate.distinct.then(|| HashSet::from([value.clone()]));

                Self::Count {
                    wildcard: true,
                    count: 1,
                    distinct_values,
                }
            }
            AggregateFunction::Count(CountArgExpr::Expr(_)) => {
                let mut distinct_values = aggregate.distinct.then(HashSet::new);

                if let Some(set) = distinct_values.as_mut()
                    && !value.is_null()
                {
                    set.insert(value.clone());
                }

                Self::Count {
                    wildcard: false,
                    count: i64::from(!value.is_null()),
                    distinct_values,
                }
            }
            AggregateFunction::Sum(_) => {
                let mut distinct_values = aggregate.distinct.then(HashSet::new);

                if let Some(set) = distinct_values.as_mut() {
                    set.insert(value.clone());
                }

                Self::Sum {
                    value,
                    distinct_values,
                }
            }
            AggregateFunction::Min(_) => {
                let mut distinct_values = aggregate.distinct.then(HashSet::new);

                if let Some(set) = distinct_values.as_mut() {
                    set.insert(value.clone());
                }

                Self::Min {
                    value,
                    distinct_values,
                }
            }
            AggregateFunction::Max(_) => {
                let mut distinct_values = aggregate.distinct.then(HashSet::new);

                if let Some(set) = distinct_values.as_mut() {
                    set.insert(value.clone());
                }

                Self::Max {
                    value,
                    distinct_values,
                }
            }
            AggregateFunction::Avg(_) => {
                let mut distinct_values = aggregate.distinct.then(HashSet::new);

                if let Some(set) = distinct_values.as_mut() {
                    set.insert(value.clone());
                }

                Self::Avg {
                    sum: value,
                    count: 1,
                    distinct_values,
                }
            }
            AggregateFunction::Variance(_) => {
                let mut distinct_values = aggregate.distinct.then(HashSet::new);

                if let Some(set) = distinct_values.as_mut() {
                    set.insert(value.clone());
                }

                Self::Variance {
                    sum_square: value.multiply(&value)?,
                    sum: value,
                    count: 1,
                    distinct_values,
                }
            }
            AggregateFunction::Stdev(_) => {
                let mut distinct_values = aggregate.distinct.then(HashSet::new);

                if let Some(set) = distinct_values.as_mut() {
                    set.insert(value.clone());
                }

                Self::Stdev {
                    sum_square: value.multiply(&value)?,
                    sum: value,
                    count: 1,
                    distinct_values,
                }
            }
        })
    }

    fn accumulate(&mut self, new_value: &Value) -> Result<bool> {
        match self {
            Self::Count {
                wildcard,
                count,
                distinct_values,
            } => {
                if !new_value.is_null() && !Self::track_distinct(distinct_values, new_value) {
                    return Ok(false);
                }

                if *wildcard || !new_value.is_null() {
                    *count += 1;
                    Ok(true)
                } else {
                    Ok(false)
                }
            }
            Self::Sum {
                value,
                distinct_values,
            } => {
                if !Self::track_distinct(distinct_values, new_value) {
                    return Ok(false);
                }

                *value = value.add(new_value)?;
                Ok(true)
            }
            Self::Min {
                value,
                distinct_values,
            } => {
                if !Self::track_distinct(distinct_values, new_value) {
                    return Ok(false);
                }

                if let Some(Ordering::Greater) = value.evaluate_cmp(new_value) {
                    *value = new_value.clone();
                }

                Ok(true)
            }
            Self::Max {
                value,
                distinct_values,
            } => {
                if !Self::track_distinct(distinct_values, new_value) {
                    return Ok(false);
                }

                if let Some(Ordering::Less) = value.evaluate_cmp(new_value) {
                    *value = new_value.clone();
                }

                Ok(true)
            }
            Self::Avg {
                sum,
                count,
                distinct_values,
            } => {
                if !Self::track_distinct(distinct_values, new_value) {
                    return Ok(false);
                }

                *sum = sum.add(new_value)?;
                *count += 1;
                Ok(true)
            }
            Self::Variance {
                sum_square,
                sum,
                count,
                distinct_values,
            } => {
                if !Self::track_distinct(distinct_values, new_value) {
                    return Ok(false);
                }

                *sum_square = sum_square.add(&new_value.multiply(new_value)?)?;
                *sum = sum.add(new_value)?;
                *count += 1;
                Ok(true)
            }
            Self::Stdev {
                sum_square,
                sum,
                count,
                distinct_values,
            } => {
                if !Self::track_distinct(distinct_values, new_value) {
                    return Ok(false);
                }

                *sum_square = sum_square.add(&new_value.multiply(new_value)?)?;
                *sum = sum.add(new_value)?;
                *count += 1;
                Ok(true)
            }
        }
    }

    fn export(self) -> Result<Value> {
        let variance = |sum_square: Value, sum: Value, count: i64| -> Result<Value> {
            let count = Value::I64(count);
            let sum_expr1 = sum_square.multiply(&count)?;
            let sum_expr2 = sum.multiply(&sum)?;
            let expr_sub = sum_expr1.cast(&DataType::Float)?.subtract(&sum_expr2)?;
            let cnt_square = count.multiply(&count)?;
            expr_sub.divide(&cnt_square)
        };

        match self {
            Self::Count { count, .. } => Ok(Value::I64(count)),
            Self::Sum { value, .. } | Self::Min { value, .. } | Self::Max { value, .. } => {
                Ok(value)
            }
            Self::Avg { sum, count, .. } => {
                let sum = sum.cast(&DataType::Float)?;
                sum.divide(&Value::I64(count))
            }
            Self::Variance {
                sum_square,
                sum,
                count,
                ..
            } => variance(sum_square, sum, count),
            Self::Stdev {
                sum_square,
                sum,
                count,
                ..
            } => variance(sum_square, sum, count)?.sqrt(),
        }
    }
}

struct GroupState<'a> {
    representative: Option<Arc<RowContext<'a>>>,
    values: Vec<Option<AggrValue>>,
}

impl<'a> GroupState<'a> {
    fn new(slot_count: usize, representative: Option<Arc<RowContext<'a>>>) -> Self {
        Self {
            representative,
            values: vec![None; slot_count],
        }
    }
}

pub struct State<'a, T: GStore> {
    storage: &'a T,
    slot_count: usize,
    groups: Vec<GroupState<'a>>,
    group_indexes: HashMap<Vec<Value>, usize>,
}

impl<'a, T: GStore> State<'a, T> {
    pub fn new(storage: &'a T, slot_count: usize, global: bool) -> Self {
        let mut groups = Vec::new();
        let mut group_indexes = HashMap::new();

        if global {
            groups.push(GroupState::new(slot_count, None));
            group_indexes.insert(Vec::new(), 0);
        }

        Self {
            storage,
            slot_count,
            groups,
            group_indexes,
        }
    }

    pub fn apply(&mut self, group: Vec<Value>, context: Arc<RowContext<'a>>) -> usize {
        if let Some(index) = self.group_indexes.get(&group).copied() {
            if self.groups[index].representative.is_none() {
                self.groups[index].representative = Some(context);
            }

            return index;
        }

        let index = self.groups.len();
        self.groups
            .push(GroupState::new(self.slot_count, Some(Arc::clone(&context))));
        self.group_indexes.insert(group, index);

        index
    }

    pub async fn accumulate(
        &mut self,
        group_index: usize,
        filter_context: Option<Arc<RowContext<'a>>>,
        slot: usize,
        aggregate: &Aggregate,
    ) -> Result<()> {
        let value = match &aggregate.func {
            AggregateFunction::Count(CountArgExpr::Wildcard) => {
                if aggregate.distinct {
                    let context = filter_context.as_ref().ok_or_else(|| {
                        EvaluateError::FilterContextRequiredForAggregate(Box::new(
                            aggregate.clone(),
                        ))
                    })?;
                    let entries = context.get_all_entries();
                    let values: Vec<Value> = entries.into_iter().map(|(_, value)| value).collect();
                    Value::List(values)
                } else {
                    Value::Null
                }
            }
            AggregateFunction::Count(CountArgExpr::Expr(expr))
            | AggregateFunction::Sum(expr)
            | AggregateFunction::Min(expr)
            | AggregateFunction::Max(expr)
            | AggregateFunction::Avg(expr)
            | AggregateFunction::Variance(expr)
            | AggregateFunction::Stdev(expr) => evaluate(self.storage, filter_context, None, expr)
                .await?
                .try_into()?,
        };

        let group = self
            .groups
            .get_mut(group_index)
            .expect("group index must exist");
        match group.values[slot].as_mut() {
            Some(aggr_value) => {
                aggr_value.accumulate(&value)?;
            }
            None => {
                group.values[slot] = Some(AggrValue::new(aggregate, &value)?);
            }
        }

        Ok(())
    }

    pub fn export(self, aggregate_slots: &[Aggregate]) -> Result<Vec<AggregateContext<'a>>> {
        let groups = self.groups;

        groups
            .into_iter()
            .map(|group| {
                let values = if aggregate_slots.is_empty() {
                    None
                } else {
                    let values = group
                        .values
                        .into_iter()
                        .zip(aggregate_slots.iter())
                        .map(|(value, aggregate)| match value {
                            Some(value) => value.export(),
                            None => Ok(empty_value(aggregate)),
                        })
                        .collect::<Result<Vec<_>>>()?;

                    Some(Arc::new(AggregateValues::new(values)))
                };

                Ok(AggregateContext {
                    aggregated: values,
                    next: group.representative,
                })
            })
            .collect()
    }
}

fn empty_value(aggregate: &Aggregate) -> Value {
    match aggregate.func {
        AggregateFunction::Count(_) => Value::I64(0),
        AggregateFunction::Sum(_)
        | AggregateFunction::Min(_)
        | AggregateFunction::Max(_)
        | AggregateFunction::Avg(_)
        | AggregateFunction::Variance(_)
        | AggregateFunction::Stdev(_) => Value::Null,
    }
}
