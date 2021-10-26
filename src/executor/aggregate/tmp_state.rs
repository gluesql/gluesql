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

enum AggrState {
    Count(Value),
    Sum(Value),
    Min(Value),
    Max(Value),
    Avg {
        sum: Value,
        count: Value,
    }
}

impl AggrState {
    //do something
}

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
                    .map(|((_, aggr), (_, v1, v2))| {
                        // v.2 : exists to store the intermediate value of the avg function.
                        if !v1.is_null() && !v2.is_null() {
                            (aggr, v2)
                        } else {
                            (aggr, v1)
                        }
                    })
                    .collect::<HashMap<&'a Aggregate, Value>>();
                let next = contexts.get(i).map(Rc::clone);

                (Some(aggregated), next)
            })
            .collect::<Vec<(Option<ValuesMap<'a>>, Option<Rc<BlendContext<'a>>>)>>()
    }

    pub fn set_aggr_state(aggr: &Box<Aggregate>, expr: &Expr) -> Result<Self>{
        match aggr.as_ref() {
            Aggregate::Count(expr) => AggrState::Count(expr),
            Aggregate::Sum(expr) => AggrState::Sum(expr),
            Aggregate::Min(expr) => AggrState::Min(get_value(expr)),
            Aggregate::Max(expr) => AggrState::Max(get_value(expr)),
            Aggregate::Avg(expr) => AggrState::Avg(get_value(expr)),
            _ => Ok(state)
        }
    }
}
