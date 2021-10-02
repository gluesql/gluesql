use {
    super::hash::GroupKey,
    crate::{
        ast::Aggregate,
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

pub struct State<'a> {
    index: usize,
    group: Group,
    values: IndexMap<(Group, &'a Aggregate), (usize, Value, Value)>,
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
        let (values, _) = self.values.insert(key, (self.index, value.0, value.1));
        Self {
            index: self.index,
            group: self.group,
            values,
            groups: self.groups,
            contexts: self.contexts,
        }
    }

    fn get(&self, aggr: &'a Aggregate) -> Option<&(usize, Value, Value)> {
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
                    .map(|((_, aggr), v)| {
                        //v.2 : exists to store the intermediate value of the avg function.
                        if !v.1.is_null() && !v.2.is_null() {
                            (aggr, v.2)
                        } else {
                            (aggr, v.1)
                        }
                    })
                    .collect::<HashMap<&'a Aggregate, Value>>();
                let next = contexts.get(i).map(Rc::clone);
                (Some(aggregated), next)
            })
            .collect::<Vec<(Option<ValuesMap<'a>>, Option<Rc<BlendContext<'a>>>)>>()
    }

    pub fn add(self, aggr: &'a Aggregate, target: &Value) -> Result<Self> {
        let value = match self.get(aggr) {
            Some(v) => {
                if self.index <= v.0 {
                    return Ok(self);
                }
                target.add(&v.1)?
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

            match v.1.partial_cmp(target) {
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

            match v.1.partial_cmp(target) {
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
                target.add(&v.1)?
            }
            None => target.clone(),
        };
        let divided_value = match self.get(aggr) {
            Some(v) => added_value.divide(&Value::I64((v.0 as i64) + 2))?,
            None => target.clone(),
        };
        Ok(self.update(aggr, (added_value, divided_value)))
    }
}
