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
    values: IndexMap<(Group, &'a Aggregate), (usize, Value)>,
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

    fn update(self, aggr: &'a Aggregate, value: Value) -> Self {
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

    fn get(&self, aggr: &'a Aggregate) -> Option<&(usize, Value)> {
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
            .map(|(k, (_, v))| (k, v))
            .chunks(size)
            .into_iter()
            .enumerate()
            .map(|(i, entries)| {
                let aggregated = entries
                    .map(|((_, aggr), value)| (aggr, value))
                    .collect::<HashMap<&'a Aggregate, Value>>();
                let next = contexts.get(i).map(Rc::clone);

                (Some(aggregated), next)
            })
            .collect::<Vec<(Option<ValuesMap<'a>>, Option<Rc<BlendContext<'a>>>)>>()
    }

    pub fn add(self, aggr: &'a Aggregate, target: &Value) -> Result<Self> {
        let value = match self.get(aggr) {
            Some((index, value)) => {
                if &self.index <= index {
                    return Ok(self);
                }

                target.add(value)?
            }
            None => target.clone(),
        };

        Ok(self.update(aggr, value))
    }

    pub fn set_max(self, aggr: &'a Aggregate, target: &Value) -> Self {
        if let Some((index, value)) = self.get(aggr) {
            if &self.index <= index {
                return self;
            }

            match value.partial_cmp(target) {
                None | Some(Ordering::Greater) | Some(Ordering::Equal) => {
                    return self;
                }
                Some(Ordering::Less) => (),
            }
        };

        self.update(aggr, target.clone())
    }

    pub fn set_min(self, aggr: &'a Aggregate, target: &Value) -> Self {
        if let Some((index, value)) = self.get(aggr) {
            if &self.index <= index {
                return self;
            }

            match value.partial_cmp(target) {
                None | Some(Ordering::Less) => {
                    return self;
                }
                Some(Ordering::Equal) | Some(Ordering::Greater) => (),
            }
        }

        self.update(aggr, target.clone())
    }
}
