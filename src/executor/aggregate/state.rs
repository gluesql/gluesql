use im_rc::{HashMap, HashSet};
use std::cmp::Ordering;
use std::rc::Rc;

use sqlparser::ast::Function;

use crate::utils::{ImVector, IndexMap};

use super::hash::GroupKey;
use crate::data::Value;
use crate::executor::context::BlendContext;
use crate::result::Result;

type Key<'a> = (Rc<Vec<GroupKey>>, &'a Function);
type ValuesMap<'a> = HashMap<&'a Function, Value>;

pub struct State<'a> {
    index: usize,
    group: Rc<Vec<GroupKey>>,
    values: IndexMap<Key<'a>, (usize, Value)>,
    groups: HashSet<Rc<Vec<GroupKey>>>,
    contexts: ImVector<Rc<BlendContext<'a>>>,
}

impl<'a> State<'a> {
    pub fn new() -> Self {
        State {
            index: 0,
            group: Rc::new(vec![GroupKey::Null]),
            values: IndexMap::new(),
            contexts: ImVector::new(),
            groups: HashSet::new(),
        }
    }

    pub fn apply(self, group: Vec<GroupKey>, context: Rc<BlendContext<'a>>, index: usize) -> Self {
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
            contexts,
            groups,
        }
    }

    fn update(self, func: &'a Function, value: Value) -> Self {
        let key = (Rc::clone(&self.group), func);
        let (values, _) = self.values.insert(key, (self.index, value));

        Self {
            index: self.index,
            group: self.group,
            values,
            contexts: self.contexts,
            groups: self.groups,
        }
    }

    fn get(&self, key: &'a Function) -> Option<&(usize, Value)> {
        let group = Rc::clone(&self.group);

        self.values.get(&(group, key))
    }

    pub fn export(self) -> Vec<(Option<ValuesMap<'a>>, Option<Rc<BlendContext<'a>>>)> {
        let size = match self.values.keys().next() {
            Some((target, _)) => match self.values.keys().position(|(key, _)| key != target) {
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
            .collect::<Vec<(Key<'a>, Value)>>()
            .chunks(size)
            .enumerate()
            .map(|(i, entries)| {
                let aggregated = entries
                    .into_iter()
                    .map(|(key, value)| {
                        // TODO: remove value.clone()
                        (key.1, value.clone())
                    })
                    .collect::<HashMap<&'a Function, Value>>();
                let next = contexts.get(i).map(Rc::clone);

                (Some(aggregated), next)
            })
            .collect::<Vec<(Option<ValuesMap<'a>>, Option<Rc<BlendContext<'a>>>)>>()
    }

    pub fn add(self, key: &'a Function, target: &Value) -> Result<Self> {
        let value = match self.get(key) {
            Some((index, value)) => {
                if &self.index <= index {
                    return Ok(self);
                }

                target.add(value)?
            }
            None => target.clone(),
        };

        Ok(self.update(key, value))
    }

    pub fn set_max(self, key: &'a Function, target: &Value) -> Self {
        if let Some((index, value)) = self.get(key) {
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

        self.update(key, target.clone())
    }

    pub fn set_min(self, key: &'a Function, target: &Value) -> Self {
        if let Some((index, value)) = self.get(key) {
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

        self.update(key, target.clone())
    }
}
