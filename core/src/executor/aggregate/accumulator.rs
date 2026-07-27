use {
    crate::{
        ast::DataType,
        data::Value,
        plan::{AggregateFunctionPlan, AggregatePlan, CountArgExprPlan},
        result::Result,
    },
    std::{cmp::Ordering, collections::HashSet},
};

/// Accumulates a single aggregate value across the rows that belong to it.
///
/// Shared by `GROUP BY` aggregation (`executor::aggregate::state`) and window
/// aggregate functions (`executor::window`) so both compute identical `NULL`
/// semantics.
#[derive(Clone)]
pub(crate) enum AggrValue {
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

    pub(crate) fn new(aggregate: &AggregatePlan, value: &Value) -> Result<Self> {
        let value = value.clone();

        Ok(match &aggregate.func {
            AggregateFunctionPlan::Count(CountArgExprPlan::Wildcard) => {
                let distinct_values = aggregate.distinct.then(|| HashSet::from([value.clone()]));

                Self::Count {
                    wildcard: true,
                    count: 1,
                    distinct_values,
                }
            }
            AggregateFunctionPlan::Count(CountArgExprPlan::Expr(_)) => {
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
            AggregateFunctionPlan::Sum(_) => {
                let mut distinct_values = aggregate.distinct.then(HashSet::new);

                if let Some(set) = distinct_values.as_mut() {
                    set.insert(value.clone());
                }

                Self::Sum {
                    value,
                    distinct_values,
                }
            }
            AggregateFunctionPlan::Min(_) => {
                let mut distinct_values = aggregate.distinct.then(HashSet::new);

                if let Some(set) = distinct_values.as_mut() {
                    set.insert(value.clone());
                }

                Self::Min {
                    value,
                    distinct_values,
                }
            }
            AggregateFunctionPlan::Max(_) => {
                let mut distinct_values = aggregate.distinct.then(HashSet::new);

                if let Some(set) = distinct_values.as_mut() {
                    set.insert(value.clone());
                }

                Self::Max {
                    value,
                    distinct_values,
                }
            }
            AggregateFunctionPlan::Avg(_) => {
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
            AggregateFunctionPlan::Variance(_) => {
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
            AggregateFunctionPlan::Stdev(_) => {
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

    pub(crate) fn accumulate(&mut self, new_value: &Value) -> Result<bool> {
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
            }
            | Self::Stdev {
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

    pub(crate) fn export(self) -> Result<Value> {
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

pub(crate) fn empty_value(aggregate: &AggregatePlan) -> Value {
    match &aggregate.func {
        AggregateFunctionPlan::Count(_) => Value::I64(0),
        AggregateFunctionPlan::Sum(_)
        | AggregateFunctionPlan::Min(_)
        | AggregateFunctionPlan::Max(_)
        | AggregateFunctionPlan::Avg(_)
        | AggregateFunctionPlan::Variance(_)
        | AggregateFunctionPlan::Stdev(_) => Value::Null,
    }
}
