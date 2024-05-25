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
    },
    Sum(Value),
    Min(Value),
    Max(Value),
    Avg {
        sum: Value,
        count: i64,
    },
    Variance {
        sum_square: Value,
        sum: Value,
        count: i64,
    },
    Stdev {
        sum_square: Value,
        sum: Value,
        count: i64,
    },
}

impl AggrValue {
    fn new(aggr: &Aggregate, value: &Value) -> Result<Self> {
        let value = value.clone();

        Ok(match aggr {
            Aggregate::Count(CountArgExpr::Wildcard) => AggrValue::Count {
                wildcard: true,
                count: 1,
            },
            Aggregate::Count(CountArgExpr::Expr(_)) => AggrValue::Count {
                wildcard: false,
                count: i64::from(!value.is_null()),
            },
            Aggregate::Sum(_) => AggrValue::Sum(value),
            Aggregate::Min(_) => AggrValue::Min(value),
            Aggregate::Max(_) => AggrValue::Max(value),
            Aggregate::Avg(_) => AggrValue::Avg {
                sum: value,
                count: 1,
            },
            Aggregate::Variance(_) => AggrValue::Variance {
                sum_square: value.multiply(&value)?,
                sum: value,
                count: 1,
            },
            Aggregate::Stdev(_) => AggrValue::Stdev {
                sum_square: value.multiply(&value)?,
                sum: value,
                count: 1,
            },
        })
    }

    fn accumulate(&self, new_value: &Value) -> Result<Option<Self>> {
        match self {
            Self::Count { wildcard, count } => {
                let wildcard = *wildcard;

                if wildcard || !new_value.is_null() {
                    Ok(Some(AggrValue::Count {
                        wildcard,
                        count: count + 1,
                    }))
                } else {
                    Ok(None)
                }
            }
            Self::Sum(value) => Ok(Some(Self::Sum(value.add(new_value)?))),
            Self::Min(value) => match &value.evaluate_cmp(new_value) {
                Some(Ordering::Greater) => Ok(Some(Self::Min(new_value.clone()))),
                _ => Ok(None),
            },
            Self::Max(value) => match &value.evaluate_cmp(new_value) {
                Some(Ordering::Less) => Ok(Some(Self::Max(new_value.clone()))),
                _ => Ok(None),
            },
            Self::Avg { sum, count } => Ok(Some(Self::Avg {
                sum: sum.add(new_value)?,
                count: count + 1,
            })),
            Self::Variance {
                sum_square,
                sum,
                count,
            } => Ok(Some(Self::Variance {
                sum_square: sum_square.add(&new_value.multiply(new_value)?)?,
                sum: sum.add(new_value)?,
                count: count + 1,
            })),
            Self::Stdev {
                sum_square,
                sum,
                count,
            } => Ok(Some(Self::Stdev {
                sum_square: sum_square.add(&new_value.multiply(new_value)?)?,
                sum: sum.add(new_value)?,
                count: count + 1,
            })),
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
            Self::Sum(value) | Self::Min(value) | Self::Max(value) => Ok(value),
            Self::Avg { sum, count } => {
                let sum = sum.cast(&DataType::Float)?;

                sum.divide(&Value::I64(count))
            }
            Self::Variance {
                sum_square,
                sum,
                count,
            } => variance(sum_square, sum, count).await,
            Self::Stdev {
                sum_square,
                sum,
                count,
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
            Aggregate::Count(CountArgExpr::Wildcard) => Value::Null,
            Aggregate::Count(CountArgExpr::Expr(expr))
            | Aggregate::Sum(expr)
            | Aggregate::Min(expr)
            | Aggregate::Max(expr)
            | Aggregate::Avg(expr)
            | Aggregate::Variance(expr)
            | Aggregate::Stdev(expr) => evaluate(self.storage, filter_context, None, expr)
                .await?
                .try_into()?,
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
