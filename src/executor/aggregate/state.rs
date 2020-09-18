use im_rc::{HashMap, HashSet};
use itertools::Itertools;
use std::cmp::Ordering;
use std::rc::Rc;

use sqlparser::ast::Function;

use crate::utils::{IndexMap, Vector};

use super::hash::GroupKey;
use crate::data::Value;
use crate::executor::context::BlendContext;
use crate::result::Result;

type Group = Rc<Vec<GroupKey>>;
type ValuesMap<'a> = HashMap<&'a Function, Value>;

pub struct State<'a> {
    index: usize,
    group: Group,
    values: IndexMap<(Group, &'a Function), (usize, Value)>,
    groups: HashSet<Group>,
    contexts: Vector<Rc<BlendContext<'a>>>,
}

impl<'a> State<'a> {
    pub fn new() -> Self {
        State {
            index: 0,
            group: Rc::new(vec![GroupKey::Null]),
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

    fn update(self, func: &'a Function, value: Value) -> Self {
        let key = (Rc::clone(&self.group), func);
        let (values, _) = self.values.insert(key, (self.index, value));

        Self {
            index: self.index,
            group: self.group,
            values,
            groups: self.groups,
            contexts: self.contexts,
        }
    }

    fn get(&self, func: &'a Function) -> Option<&(usize, Value)> {
        let group = Rc::clone(&self.group);

        self.values.get(&(group, func))
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
                    .map(|((_, func), value)| (func, value))
                    .collect::<HashMap<&'a Function, Value>>();
                let next = contexts.get(i).map(Rc::clone);

                (Some(aggregated), next)
            })
            .collect::<Vec<(Option<ValuesMap<'a>>, Option<Rc<BlendContext<'a>>>)>>()
    }

    pub fn add(self, func: &'a Function, target: &Value) -> Result<Self> {
        let value = match self.get(func) {
            Some((index, value)) => {
                if &self.index <= index {
                    return Ok(self);
                }

                target.add(value)?
            }
            None => target.clone(),
        };

        Ok(self.update(func, value))
    }

    pub fn set_max(self, func: &'a Function, target: &Value) -> Self {
        if let Some((index, value)) = self.get(func) {
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

        self.update(func, target.clone())
    }

    pub fn set_min(self, func: &'a Function, target: &Value) -> Self {
        if let Some((index, value)) = self.get(func) {
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

        self.update(func, target.clone())
    }
}
