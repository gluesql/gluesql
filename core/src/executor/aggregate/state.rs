use {
    super::error::AggregateError,
    crate::{
        ast::{Aggregate, BinaryOperator, CountArgExpr, Expr},
        data::{Key, Value},
        executor::context::BlendContext,
        result::Result,
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
            group: Rc::new(vec![Key::None]),
            values: IndexMap::new(),
            groups: HashSet::new(),
            contexts: Vector::new(),
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

    fn accumulate_get_value1(
        &self,
        context: &'a BlendContext<'_>,
        expr: &Expr,
    ) -> Result<&'a Value> {
        match expr {
            Expr::Identifier(ident) => match context.get_value(ident) {
                Some(x) => Ok(x),
                None => Err(AggregateError::ValueNotFound(ident.to_string()).into()),
            },
            Expr::CompoundIdentifier(idents) => {
                if idents.len() != 2 {
                    return Err(AggregateError::UnsupportedCompoundIdentifier(expr.clone()).into());
                }

                let table_alias = &idents[0];
                let column = &idents[1];

                match context.get_alias_value(table_alias, column) {
                    Some(x) => Ok(x),
                    None => Err(AggregateError::ValueNotFound(column.to_string()).into()),
                }
            }
            _ => Err(AggregateError::OnlyIdentifierAllowed.into()),
        }
    }

    fn accumulate_get_value(&self, context: &BlendContext<'_>, expr: &Expr) -> Result<Value> {
        match expr {
            Expr::Identifier(_ident) => match self.accumulate_get_value1(context, expr) {
                Ok(x) => {
                    let y: Value = x.to_owned();
                    Ok(y)
                }
                Err(x) => Err(x),
            },
            Expr::CompoundIdentifier(_idents) => match self.accumulate_get_value1(context, expr) {
                Ok(x) => {
                    let y: Value = x.to_owned();
                    Ok(y)
                }
                Err(x) => Err(x),
            },
            Expr::BinaryOp { left, op, right } => {
                let left_value: &Value = self.accumulate_get_value1(context, left)?;
                let right_value: &Value = self.accumulate_get_value1(context, right)?;

                //most aggregate functions ignore NUlls.
                let left_value: &Value = match *left_value {
                    Value::Null => Value::I8(0).as_ref(),
                    _ => left_value,
                };

                let right_value: &Value = match *right_value {
                    Value::Null => Value::I8(0).as_ref(),
                    _ => right_value,
                };

                println!("{:#?} {:#?} {:#?}", left_value, op, right_value);
                match op {
                    BinaryOperator::Plus => left_value.add(right_value),
                    BinaryOperator::Minus => left_value.subtract(right_value),
                    BinaryOperator::Multiply => left_value.multiply(right_value),
                    BinaryOperator::Divide => left_value.divide(right_value),
                    BinaryOperator::Modulo => left_value.modulo(right_value),
                    _ => Err(AggregateError::UnsupportedAggregateFunction.into()),
                }
            }
            _ => Err(AggregateError::UnsupportedAggregateFunction.into()),
        }
    }

    pub fn accumulate(self, context: &BlendContext<'_>, aggr: &'a Aggregate) -> Result<Self> {
        let value = match aggr {
            Aggregate::Count(CountArgExpr::Wildcard) => Value::Null,
            Aggregate::Count(CountArgExpr::Expr(expr))
            | Aggregate::Sum(expr)
            | Aggregate::Min(expr)
            | Aggregate::Max(expr)
            | Aggregate::Avg(expr) => self.accumulate_get_value(context, expr)?,
        };

        let aggr_value = match self.get(aggr) {
            Some((index, _)) if self.index <= *index => None,
            Some((_, aggr_value)) => aggr_value.accumulate(&value)?,
            None => Some(AggrValue::new(aggr, &value)),
        };

        match aggr_value {
            Some(aggr_value) => Ok(self.update(aggr, aggr_value)),
            None => Ok(self),
        }
    }
}
