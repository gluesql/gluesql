use {
    super::{error::AggregateError, hash::GroupKey},
    crate::{
        ast::{Aggregate, Expr},
        data::Value,
        executor::context::BlendContext,
        result::Result,
        utils::{IndexMap, Vector},
    },
    im_rc::{HashMap, HashSet},
    itertools::Itertools,
    std::{cmp::Ordering, rc::Rc},
};
type Group = Rc<Vec<GroupKey>>;
type ValuesMap<'a> = HashMap<&'a Aggregate, Value>;
type Context<'a> = Rc<BlendContext<'a>>;
enum AggrValue {
    Count { wildcard: bool, count: i64 },
    Sum(Value),
    Min(Value),
    Max(Value),
    Avg { sum: Value, count: i64 },
}

impl<'a> AggrValue {
    fn new(aggr: &Aggregate, value: &Value) -> Self {
        let value = value.clone();

        match aggr {
            Aggregate::Count(expr) => AggrValue::Count {
                wildcard: matches!(expr, Expr::Wildcard),
                count: 1,
            },
            Aggregate::Sum(_) => AggrValue::Sum(value),
            Aggregate::Min(_) => AggrValue::Min(value),
            Aggregate::Max(_) => AggrValue::Max(value),
            Aggregate::Avg(_) => AggrValue::Avg {
                sum: value,
                count: 1,
            },
        }
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
            Self::Sum(value) => Ok(Some(Self::Sum(value.add(new_value).unwrap()))),
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
        }
    }

    fn export(self) -> Result<Value> {
        match self {
            Self::Count { count, .. } => Ok(Value::I64(count)),
            Self::Sum(value) | Self::Min(value) | Self::Max(value) => Ok(value),
            Self::Avg { sum, count } => sum.divide(&Value::I64(count)),
        }
    }
}

pub struct State<'a> {
    index: usize,
    group: Group,
    values: IndexMap<(Group, &'a Aggregate), (usize, AggrValue)>,
    groups: HashSet<Group>,
    contexts: Vector<Rc<BlendContext<'a>>>,
}

impl<'a> State<'a> {
    pub fn new() -> Self {
        State {
            index: 0,
            group: Rc::new(vec![GroupKey::None]),
            values: IndexMap::new(),
            groups: HashSet::new(),
            contexts: Vector::new(),
        }
    }

    pub fn apply(self, index: usize, group: Vec<GroupKey>, context: Rc<BlendContext<'a>>) -> Self {
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

    pub fn accumulate(self, context: &BlendContext<'_>, aggr: &'a Aggregate) -> Result<Self> {
        let get_value = |expr: &Expr| match expr {
            Expr::Identifier(ident) => context
                .get_value(ident)
                .ok_or_else(|| AggregateError::ValueNotFound(ident.to_string())),
            Expr::CompoundIdentifier(idents) => {
                if idents.len() != 2 {
                    return Err(AggregateError::UnsupportedCompoundIdentifier(expr.clone()));
                }

                let table_alias = &idents[0];
                let column = &idents[1];

                context
                    .get_alias_value(table_alias, column)
                    .ok_or_else(|| AggregateError::ValueNotFound(column.to_string()))
            }
            _ => Err(AggregateError::OnlyIdentifierAllowed),
        };

        let value = match aggr {
            Aggregate::Count(Expr::Wildcard) => &Value::Null,
            Aggregate::Count(expr)
            | Aggregate::Sum(expr)
            | Aggregate::Min(expr)
            | Aggregate::Max(expr)
            | Aggregate::Avg(expr) => get_value(expr)?,
        };

        let aggr_value = match self.get(aggr) {
            Some((index, _)) if self.index <= *index => None,
            Some((_, aggr_value)) => aggr_value.accumulate(value)?,
            None => Some(AggrValue::new(aggr, value)),
        };

        match aggr_value {
            Some(aggr_value) => Ok(self.update(aggr, aggr_value)),
            None => Ok(self),
        }
    }
}
