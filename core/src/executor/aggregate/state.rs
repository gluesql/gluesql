use {
    crate::{
        ast::{Aggregate, CountArgExpr},
        data::{Key, Value},
        executor::{context::BlendContext, context::FilterContext, evaluate::evaluate},
        result::Result,
        store::GStore,
    },
    im_rc::{HashMap, HashSet},
    itertools::Itertools,
    std::{cmp::Ordering, rc::Rc},
    utils::{IndexMap, Vector},
};
type Group = Rc<Vec<Key>>;
type ValuesMap<'a> = HashMap<&'a Aggregate, Value>;
type Context<'a> = Rc<BlendContext<'a>>;
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
                count: 1,
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
            Self::Min(value) => match &value.partial_cmp(new_value) {
                Some(Ordering::Greater) => Ok(Some(Self::Min(new_value.clone()))),
                _ => Ok(None),
            },
            Self::Max(value) => match &value.partial_cmp(new_value) {
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

    fn export(self) -> Result<Value> {
        let variance = |sum_square: Value, sum: Value, count: i64| {
            let sum_expr1 = sum_square.multiply(&Value::I64(count))?;
            let sum_expr2 = sum.multiply(&sum)?;
            let expr_sub = sum_expr1.subtract(&sum_expr2)?;
            let cnt_square = Value::F64(count as f64).multiply(&Value::F64(count as f64))?;
            expr_sub.divide(&cnt_square)
        };

        match self {
            Self::Count { count, .. } => Ok(Value::I64(count)),
            Self::Sum(value) | Self::Min(value) | Self::Max(value) => Ok(value),
            Self::Avg { sum, count } => sum.divide(&Value::F64(count as f64)),
            Self::Variance {
                sum_square,
                sum,
                count,
            } => variance(sum_square, sum, count),
            Self::Stdev {
                sum_square,
                sum,
                count,
            } => variance(sum_square, sum, count)?.sqrt(),
        }
    }
}

pub struct State<'a> {
    index: usize,
    group: Group,
    values: IndexMap<(Group, &'a Aggregate), (usize, AggrValue)>,
    groups: HashSet<Group>,
    contexts: Vector<Rc<BlendContext<'a>>>,
    storage: &'a dyn GStore,
}

impl<'a> State<'a> {
    pub fn new(storage: &'a dyn GStore) -> Self {
        State {
            index: 0,
            group: Rc::new(vec![Key::None]),
            values: IndexMap::new(),
            groups: HashSet::new(),
            contexts: Vector::new(),
            storage,
        }
    }

    pub fn apply(self, index: usize, group: Vec<Key>, context: Rc<BlendContext<'a>>) -> Self {
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
            values: self.values,
            groups,
            contexts,
            storage: self.storage,
        }
    }

    fn update(self, aggr: &'a Aggregate, value: AggrValue) -> Self {
        let key = (Rc::clone(&self.group), aggr);
        let (values, _) = self.values.insert(key, (self.index, value));
        Self {
            index: self.index,
            group: self.group,
            values,
            groups: self.groups,
            contexts: self.contexts,
            storage: self.storage,
        }
    }

    fn get(&self, aggr: &'a Aggregate) -> Option<&(usize, AggrValue)> {
        let group = Rc::clone(&self.group);

        self.values.get(&(group, aggr))
    }

    pub fn export(self) -> Result<Vec<(Option<ValuesMap<'a>>, Option<Context<'a>>)>> {
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

        values
            .into_iter()
            .map(|(k, v)| (k, v))
            .chunks(size)
            .into_iter()
            .enumerate()
            .map(|(i, entries)| {
                let aggregated = entries
                    .map(|((_, aggr), (_, aggr_value))| {
                        aggr_value.export().map(|value| (aggr, value))
                    })
                    .collect::<Result<HashMap<&'a Aggregate, Value>>>()?;
                let next = contexts.get(i).map(Rc::clone);

                Ok((Some(aggregated), next))
            })
            .collect::<Result<Vec<(Option<ValuesMap<'a>>, Option<Rc<BlendContext<'a>>>)>>>()
    }

    pub async fn accumulate(
        self,
        filter_context: Option<Rc<FilterContext<'a>>>,
        aggr: &'a Aggregate,
    ) -> Result<State<'a>> {
        let value = match aggr {
            Aggregate::Count(CountArgExpr::Wildcard) => Value::Null,
            Aggregate::Count(CountArgExpr::Expr(expr))
            | Aggregate::Sum(expr)
            | Aggregate::Min(expr)
            | Aggregate::Max(expr)
            | Aggregate::Avg(expr)
            | Aggregate::Variance(expr)
            | Aggregate::Stdev(expr) => {
                // let filter_context =
                //     Some(FilterContext::concat(filter_context, Some(blend_context))).map(Rc::new);
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
