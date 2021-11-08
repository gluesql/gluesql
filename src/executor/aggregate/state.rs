use {
    super::hash::GroupKey,
    crate::{
        ast::{Aggregate, Expr, SelectItem},
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

enum AggrValue {
    Count(Value),
    Sum(Value),
    Min(Value),
    Max(Value),
    Avg {
        sum: Value,
        count: Value,
    },
}

// impl <'a> AggrValue {
//     pub fn add(state: State, aggr: &'a Aggregate, target: Value) -> Self {
//         println!("{:#?}, {:#?}", aggr, target);
//         let value = match state.get(aggr) {
//             Some(v) => {
//                 if state.index <= v.0 {
//                     return AggrValue::Count(target);
//                 }
//                 target.add(&v.1)?
//             }
//             None => target.clone(),
//         };

//         AggrValue::Count(value)
//     }

//     pub fn export_aggr_value(aggr_value: AggrValue) -> Value {
//         return Value::I64(12345);
//     }

// }
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

    fn update(self, aggr: &'a Aggregate, value: (Value, Value)) -> Self {
        let key = (Rc::clone(&self.group), aggr);

        let (values, _) = match &aggr {
            Aggregate::Count(expr) => self.values.insert(key, (self.index, AggrValue::Count(Value::I64(self.index as i64 + 1)))),
            Aggregate::Sum(expr) => self.values.insert(key, (self.index, AggrValue::Sum(value.0))),
            Aggregate::Min(expr) => self.values.insert(key, (self.index, AggrValue::Min(value.0))),
            Aggregate::Max(expr) => self.values.insert(key, (self.index, AggrValue::Max(value.0))),
            Aggregate::Avg(expr) => self.values.insert(key, (self.index, AggrValue::Avg {sum:value.0, count:value.1})),
        };
        
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

    pub fn export(self) -> Vec<(Option<ValuesMap<'a>>, Option<Rc<BlendContext<'a>>>)> {
        let size = match self.values.keys().next() {
            Some((target, _)) => match self.values.keys().position(|(group, _)| group != target) {
                Some(size) => size,
                None => self.values.len(),
            },
            None => {
                return self.contexts.into_iter().map(|c| (None, Some(c))).collect();
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
                            let value = match aggr_value {
                                AggrValue::Count(x) => x,
                                AggrValue::Sum(x) => x,
                                AggrValue::Min(x) => x,
                                AggrValue::Max(x) => x,
                                AggrValue::Avg {sum : x, count: y} => x.divide(&y).unwrap(),
                            };
                            (aggr, value)
                        }

                        // {
                        //     // v.2 : exists to store the intermediate value of the avg function.
                        //     if !v1.is_null() && !v2.is_null() {
                        //         (aggr, v2)
                        //     } else {
                        //         (aggr, v1)
                        //     }
                        // }
                    )
                    .collect::<HashMap<&'a Aggregate, Value>>();
                let next = contexts.get(i).map(Rc::clone);

                (Some(aggregated), next)
            })
            .collect::<Vec<(Option<ValuesMap<'a>>, Option<Rc<BlendContext<'a>>>)>>()
    }

    pub fn set_count(self, aggr: &'a Aggregate, expr: &Expr, target: &Value) -> Result<Self> {
        let value = Value::I64(match expr {
            Expr::Wildcard => 1,
            _ => {
                if target.is_null() {
                    0
                } else {
                    1
                }
            }
        });
        Ok(self.update(aggr, (value, Value::Null)))
    }

    pub fn add(self, aggr: &'a Aggregate, target: &Value) -> Result<Self> {
        let value = match self.get(aggr) {
            Some(v) => {
                if self.index <= v.0 {
                    return Ok(self);
                }
                let a = match &v.1 {
                    AggrValue::Sum(val) => target.add(&val)?,
                    _ => Value::Null,
                };
                a
            }
            None => target.clone(),
        };

        Ok(self.update(aggr, (value, Value::Null)))
    }

    pub fn set_max(self, aggr: &'a Aggregate, target: &Value) -> Self {
        if let Some(v) = self.get(aggr) {
            if self.index <= v.0 {
                return self;
            }
            let a = match &v.1 {
                AggrValue::Max(val) => val,
                _ => &Value::Null,
            };
            match &a.partial_cmp(target) {
                None | Some(Ordering::Greater) | Some(Ordering::Equal) => {
                    return self;
                }
                Some(Ordering::Less) => (),
            }
        };

        self.update(aggr, (target.clone(), Value::Null))
    }

    pub fn set_min(self, aggr: &'a Aggregate, target: &Value) -> Self {
        if let Some(v) = self.get(aggr) {
            if self.index <= v.0 {
                return self;
            }
            let a = match &v.1 {
                AggrValue::Min(val) => val,
                _ => &Value::Null,
            };
            match &a.partial_cmp(target) {
                None | Some(Ordering::Less) => {
                    return self;
                }
                Some(Ordering::Equal) | Some(Ordering::Greater) => (),
            }
        }

        self.update(aggr, (target.clone(), Value::Null))
    }

    pub fn set_avg(self, aggr: &'a Aggregate, target: &Value) -> Result<Self> {
        let added_value = match self.get(aggr) {
            Some(v) => {
                if self.index <= v.0 {
                    return Ok(self);
                }
                let a = match &v.1 {
                    AggrValue::Sum(val) => target.add(&val)?,
                    _ => Value::Null,
                };
                a
            }
            None => target.clone(),
        };
        let divided_value = match self.get(aggr) {
            Some(v) => added_value.divide(&Value::I64((v.0 as i64) + 2))?,
            None => target.clone(),
        };
        Ok(self.update(aggr, (added_value, divided_value)))
    }
    // pub fn accumulate(self, aggr: &'a Aggregate, value: &Value) -> Result <Self> {
        
    //     let aggr_value = match &aggr {
    //         Aggregate::Count(expr) => AggrValue::add(self, aggr, *value),
    //         // Aggregate::Sum(expr) => AggrValue::sum(aggr, value),
    //         // Aggregate::Min(expr) => AggrValue::min(aggr, value),
    //         // Aggregate::Max(expr) => AggrValue::max(aggr, value),
    //         // Aggregate::Avg(expr) => AggrValue::avg(aggr, value),
    //     };
    //     return Ok(self.update(aggr,(AggrValue::export_aggr_value(aggr_value), Value::Null)))
    // }
}