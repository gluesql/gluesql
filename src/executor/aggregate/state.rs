use im_rc::HashMap;
use std::cmp::Ordering;
use std::rc::Rc;

use sqlparser::ast::Function;

use crate::utils::ImVector;

use super::hash::GroupKey;
use crate::data::Value;
use crate::executor::context::BlendContext;
use crate::result::Result;

type Key<'a> = (Rc<Vec<GroupKey>>, &'a Function);

pub struct State<'a> {
    index: usize,
    group: Rc<Vec<GroupKey>>,
    values: HashMap<Key<'a>, (usize, Value)>,
    keys: ImVector<Key<'a>>,
    contexts: HashMap<Rc<Vec<GroupKey>>, Rc<BlendContext<'a>>>,
}

impl<'a> State<'a> {
    pub fn new() -> Self {
        State {
            index: 0,
            group: Rc::new(vec![GroupKey::Null]),
            values: HashMap::new(),
            keys: ImVector::new(),
            contexts: HashMap::new(),
        }
    }

    pub fn apply(self, group: Vec<GroupKey>, context: Rc<BlendContext<'a>>, index: usize) -> Self {
        let group = Rc::new(group);
        let contexts = self.contexts.update(Rc::clone(&group), context);

        Self {
            index,
            group,
            values: self.values,
            keys: self.keys,
            contexts,
        }
    }

    fn update(self, func: &'a Function, value: Value) -> Self {
        let key = (Rc::clone(&self.group), func);
        let keys = if self.values.contains_key(&key) {
            self.keys
        } else {
            self.keys.push((Rc::clone(&self.group), func))
        };

        let values = self.values.update(key, (self.index, value));

        Self {
            index: self.index,
            group: self.group,
            values,
            keys,
            contexts: self.contexts,
        }
    }

    fn get(&self, key: &'a Function) -> Option<&(usize, Value)> {
        let group = Rc::clone(&self.group);

        self.values.get(&(group, key))
    }

    pub fn export(self) -> Vec<(HashMap<&'a Function, Value>, Option<Rc<BlendContext<'a>>>)> {
        let size = match self.keys.first() {
            Some((target, _)) => match self.keys.iter().position(|(key, _)| key != target) {
                Some(size) => size,
                None => self.keys.len(),
            },
            None => {
                return vec![];
            }
        };

        self.keys
            .chunks(size)
            .map(|keys| {
                let aggregated = keys
                    .iter()
                    .map(|key| {
                        let a: &'a Function = key.1;
                        let b: Value = self
                            .values
                            .get(key)
                            .map(|(_, value)| value.clone())
                            .unwrap();

                        (a, b)
                    })
                    .collect::<HashMap<&'a Function, Value>>();
                let next = self.contexts.get(&keys[0].0).map(Rc::clone);

                (aggregated, next)
            })
            .collect::<Vec<(HashMap<&'a Function, Value>, Option<Rc<BlendContext<'a>>>)>>()
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
