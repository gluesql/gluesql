use {
    crate::{
        ast::{Aggregate, CountArgExpr, DataType},
        data::{Key, Value},
        executor::{context::RowContext, evaluate::evaluate},
        result::Result,
        store::GStore,
    },
    futures::stream::{self, StreamExt, TryStreamExt},
    im_rc::{HashMap, HashSet},
    itertools::Itertools,
    std::{cmp::Ordering, rc::Rc},
    utils::{IndexMap, Vector},
};

type Group = Rc<Vec<Key>>;
type ValuesMap<'a> = HashMap<&'a Aggregate, Value>;
type Context<'a> = Rc<RowContext<'a>>;

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
    /// Helper function to check DISTINCT values and update set
    /// Returns (should_process, updated_distinct_values)
    fn check_distinct(
        distinct_values: Option<HashSet<Value>>,
        new_value: &Value,
    ) -> (bool, Option<HashSet<Value>>) {
        if let Some(mut set) = distinct_values {
            if set.contains(new_value) {
                return (false, Some(set));
            }
            set.insert(new_value.clone());
            (true, Some(set))
        } else {
            (true, None)
        }
    }

    fn new(aggr: &Aggregate, value: &Value) -> Result<Self> {
        let value = value.clone();

        Ok(match aggr {
            Aggregate::Count {
                expr: CountArgExpr::Wildcard,
                distinct,
            } => {
                let distinct_values = distinct.then(|| HashSet::from(&[value][..]));

                AggrValue::Count {
                    wildcard: true,
                    count: 1,
                    distinct_values,
                }
            }
            Aggregate::Count {
                expr: CountArgExpr::Expr(_),
                distinct,
            } => {
                let mut distinct_values = if *distinct {
                    Some(HashSet::new())
                } else {
                    None
                };

                if let Some(ref mut set) = distinct_values {
                    if !value.is_null() {
                        set.insert(value.clone());
                    }
                }

                AggrValue::Count {
                    wildcard: false,
                    count: i64::from(!value.is_null()),
                    distinct_values,
                }
            }
            Aggregate::Sum { expr: _, distinct } => {
                let mut distinct_values = if *distinct {
                    Some(HashSet::new())
                } else {
                    None
                };

                if let Some(ref mut set) = distinct_values {
                    set.insert(value.clone());
                }

                AggrValue::Sum {
                    value: value.clone(),
                    distinct_values,
                }
            }
            Aggregate::Min { expr: _, distinct } => {
                let mut distinct_values = if *distinct {
                    Some(HashSet::new())
                } else {
                    None
                };

                if let Some(ref mut set) = distinct_values {
                    set.insert(value.clone());
                }

                AggrValue::Min {
                    value: value.clone(),
                    distinct_values,
                }
            }
            Aggregate::Max { expr: _, distinct } => {
                let mut distinct_values = if *distinct {
                    Some(HashSet::new())
                } else {
                    None
                };

                if let Some(ref mut set) = distinct_values {
                    set.insert(value.clone());
                }

                AggrValue::Max {
                    value: value.clone(),
                    distinct_values,
                }
            }
            Aggregate::Avg { expr: _, distinct } => {
                let mut distinct_values = if *distinct {
                    Some(HashSet::new())
                } else {
                    None
                };

                if let Some(ref mut set) = distinct_values {
                    set.insert(value.clone());
                }

                AggrValue::Avg {
                    sum: value.clone(),
                    count: 1,
                    distinct_values,
                }
            }
            Aggregate::Variance { expr: _, distinct } => {
                let mut distinct_values = if *distinct {
                    Some(HashSet::new())
                } else {
                    None
                };

                if let Some(ref mut set) = distinct_values {
                    set.insert(value.clone());
                }

                AggrValue::Variance {
                    sum_square: value.multiply(&value)?,
                    sum: value.clone(),
                    count: 1,
                    distinct_values,
                }
            }
            Aggregate::Stdev { expr: _, distinct } => {
                let mut distinct_values = if *distinct {
                    Some(HashSet::new())
                } else {
                    None
                };

                if let Some(ref mut set) = distinct_values {
                    set.insert(value.clone());
                }

                AggrValue::Stdev {
                    sum_square: value.multiply(&value)?,
                    sum: value.clone(),
                    count: 1,
                    distinct_values,
                }
            }
        })
    }

    fn accumulate(&self, new_value: &Value) -> Result<Option<Self>> {
        match self {
            Self::Count {
                wildcard,
                count,
                distinct_values,
            } => {
                let wildcard = *wildcard;
                let mut distinct_values = distinct_values.clone();

                if !new_value.is_null() {
                    let (should_process, updated_distinct) =
                        Self::check_distinct(distinct_values, new_value);
                    distinct_values = updated_distinct;
                    if !should_process {
                        return Ok(None);
                    }
                }

                if wildcard || !new_value.is_null() {
                    Ok(Some(AggrValue::Count {
                        wildcard,
                        count: count + 1,
                        distinct_values,
                    }))
                } else {
                    Ok(None)
                }
            }
            Self::Sum {
                value,
                distinct_values,
            } => {
                let mut distinct_values = distinct_values.clone();

                let (should_process, updated_distinct) =
                    Self::check_distinct(distinct_values, new_value);
                distinct_values = updated_distinct;
                if !should_process {
                    return Ok(None);
                }

                Ok(Some(Self::Sum {
                    value: value.add(new_value)?,
                    distinct_values,
                }))
            }
            Self::Min {
                value,
                distinct_values,
            } => {
                let mut distinct_values = distinct_values.clone();

                let (should_process, updated_distinct) =
                    Self::check_distinct(distinct_values, new_value);
                distinct_values = updated_distinct;
                if !should_process {
                    return Ok(None);
                }

                match &value.evaluate_cmp(new_value) {
                    Some(Ordering::Greater) => Ok(Some(Self::Min {
                        value: new_value.clone(),
                        distinct_values,
                    })),
                    _ => Ok(Some(Self::Min {
                        value: value.clone(),
                        distinct_values,
                    })),
                }
            }
            Self::Max {
                value,
                distinct_values,
            } => {
                let mut distinct_values = distinct_values.clone();

                let (should_process, updated_distinct) =
                    Self::check_distinct(distinct_values, new_value);
                distinct_values = updated_distinct;
                if !should_process {
                    return Ok(None);
                }

                match &value.evaluate_cmp(new_value) {
                    Some(Ordering::Less) => Ok(Some(Self::Max {
                        value: new_value.clone(),
                        distinct_values,
                    })),
                    _ => Ok(Some(Self::Max {
                        value: value.clone(),
                        distinct_values,
                    })),
                }
            }
            Self::Avg {
                sum,
                count,
                distinct_values,
            } => {
                let mut distinct_values = distinct_values.clone();

                let (should_process, updated_distinct) =
                    Self::check_distinct(distinct_values, new_value);
                distinct_values = updated_distinct;
                if !should_process {
                    return Ok(None);
                }

                Ok(Some(Self::Avg {
                    sum: sum.add(new_value)?,
                    count: count + 1,
                    distinct_values,
                }))
            }
            Self::Variance {
                sum_square,
                sum,
                count,
                distinct_values,
            } => {
                let mut distinct_values = distinct_values.clone();

                let (should_process, updated_distinct) =
                    Self::check_distinct(distinct_values, new_value);
                distinct_values = updated_distinct;
                if !should_process {
                    return Ok(None);
                }

                Ok(Some(Self::Variance {
                    sum_square: sum_square.add(&new_value.multiply(new_value)?)?,
                    sum: sum.add(new_value)?,
                    count: count + 1,
                    distinct_values,
                }))
            }
            Self::Stdev {
                sum_square,
                sum,
                count,
                distinct_values,
            } => {
                let mut distinct_values = distinct_values.clone();

                let (should_process, updated_distinct) =
                    Self::check_distinct(distinct_values, new_value);
                distinct_values = updated_distinct;
                if !should_process {
                    return Ok(None);
                }

                Ok(Some(Self::Stdev {
                    sum_square: sum_square.add(&new_value.multiply(new_value)?)?,
                    sum: sum.add(new_value)?,
                    count: count + 1,
                    distinct_values,
                }))
            }
        }
    }

    async fn export(self) -> Result<Value> {
        let variance = |sum_square: Value, sum: Value, count: i64| async move {
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
            } => variance(sum_square, sum, count).await,
            Self::Stdev {
                sum_square,
                sum,
                count,
                ..
            } => variance(sum_square, sum, count).await?.sqrt(),
        }
    }
}

pub struct State<'a, T: GStore> {
    storage: &'a T,
    index: usize,
    group: Group,
    values: IndexMap<(Group, &'a Aggregate), (usize, AggrValue)>,
    groups: HashSet<Group>,
    contexts: Vector<Rc<RowContext<'a>>>,
}

impl<'a, T: GStore> State<'a, T> {
    pub fn new(storage: &'a T) -> Self {
        State {
            storage,
            index: 0,
            group: Rc::new(vec![Key::None]),
            values: IndexMap::new(),
            groups: HashSet::new(),
            contexts: Vector::new(),
        }
    }

    pub fn apply(self, index: usize, group: Vec<Key>, context: Rc<RowContext<'a>>) -> Self {
        let group = Rc::new(group);
        let (groups, contexts) = if self.groups.contains(&group) {
            (self.groups, self.contexts)
        } else {
            (
                self.groups.update(Rc::clone(&group)),
                self.contexts.push(context),
            )
        };

        Self {
            index,
            group,
            groups,
            contexts,
            ..self
        }
    }

    fn update(self, aggr: &'a Aggregate, value: AggrValue) -> Self {
        let key = (Rc::clone(&self.group), aggr);
        let (values, _) = self.values.insert(key, (self.index, value));
        Self { values, ..self }
    }

    fn get(&self, aggr: &'a Aggregate) -> Option<&(usize, AggrValue)> {
        let group = Rc::clone(&self.group);

        self.values.get(&(group, aggr))
    }

    pub async fn export(self) -> Result<Vec<(Option<ValuesMap<'a>>, Option<Context<'a>>)>> {
        let size = match self.values.keys().next() {
            Some((target, _)) => match self.values.keys().position(|(group, _)| group != target) {
                Some(size) => size,
                None => self.values.len(),
            },
            None => {
                return Ok(self.contexts.into_iter().map(|c| (None, Some(c))).collect());
            }
        };

        let Self {
            values, contexts, ..
        } = self;

        stream::iter(values.into_iter().chunks(size).into_iter().enumerate())
            .then(|(i, entries)| {
                let next = contexts.get(i).map(Rc::clone);

                async move {
                    let aggregated = stream::iter(entries)
                        .then(|((_, aggr), (_, aggr_value))| async move {
                            aggr_value.export().await.map(|value| (aggr, value))
                        })
                        .try_collect::<HashMap<&'a Aggregate, Value>>()
                        .await?;

                    Ok((Some(aggregated), next))
                }
            })
            .try_collect::<Vec<(Option<ValuesMap<'a>>, Option<Rc<RowContext<'a>>>)>>()
            .await
    }

    pub async fn accumulate(
        self,
        filter_context: Option<Rc<RowContext<'a>>>,
        aggr: &'a Aggregate,
    ) -> Result<State<'a, T>> {
        let value = match aggr {
            Aggregate::Count {
                expr: CountArgExpr::Wildcard,
                distinct,
            } => {
                if *distinct {
                    // For COUNT(DISTINCT *), we need to use all column values as a composite value
                    if let Some(ref context) = filter_context {
                        let entries = context.get_all_entries();
                        let values: Vec<Value> = entries.into_iter().map(|(_, v)| v).collect();
                        Value::List(values)
                    } else {
                        Value::Null
                    }
                } else {
                    Value::Null
                }
            }
            Aggregate::Count {
                expr: CountArgExpr::Expr(expr),
                distinct: _,
            }
            | Aggregate::Sum { expr, distinct: _ }
            | Aggregate::Min { expr, distinct: _ }
            | Aggregate::Max { expr, distinct: _ }
            | Aggregate::Avg { expr, distinct: _ }
            | Aggregate::Variance { expr, distinct: _ }
            | Aggregate::Stdev { expr, distinct: _ } => {
                evaluate(self.storage, filter_context, None, expr)
                    .await?
                    .try_into()?
            }
        };
        let aggr_value = match self.get(aggr) {
            Some((index, _)) if self.index <= *index => None,
            Some((_, aggr_value)) => aggr_value.accumulate(&value)?,
            None => Some(AggrValue::new(aggr, &value)?),
        };

        match aggr_value {
            Some(aggr_value) => Ok(self.update(aggr, aggr_value)),
            None => Ok(self),
        }
    }
}
