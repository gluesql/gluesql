use im_rc::HashMap;
use std::cmp::Ordering;
use std::rc::Rc;

use sqlparser::ast::Function;

use crate::utils::ImVector;

use super::hash::GroupKey;
use crate::data::Value;
use crate::result::Result;

type Key<'a> = (Rc<Vec<GroupKey>>, &'a Function);

pub struct State<'a> {
    index: usize,
    group: Rc<Vec<GroupKey>>,
    values: HashMap<Key<'a>, (usize, Value)>,
    keys: ImVector<Key<'a>>,
}

impl<'a> State<'a> {
    pub fn new() -> Self {
        State {
            index: 0,
            group: Rc::new(vec![GroupKey::Null]),
            values: HashMap::new(),
            keys: ImVector::new(),
        }
    }

    pub fn apply(self, group: Vec<GroupKey>, index: usize) -> Self {
        Self {
            index,
            group: Rc::new(group),
            values: self.values,
            keys: self.keys,
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
        }
    }

    fn get(&self, key: &'a Function) -> Option<&(usize, Value)> {
        let group = Rc::clone(&self.group);

        self.values.get(&(group, key))
    }

    pub fn export(self) -> HashMap<&'a Function, Value> {
        let size = {
            let (target, _) = self.keys.first().unwrap();

            match self.keys.iter().position(|(key, _)| key != target) {
                Some(s) => s,
                None => self.keys.len(),
            }
        };

        self.keys
            .chunks(size)
            .map(|keys| {
                keys.iter()
                    .map(|key| {
                        let a: &'a Function = key.1;
                        let b: Value = self
                            .values
                            .get(key)
                            .map(|(_, value)| value.clone())
                            .unwrap();

                        (a, b)
                    })
                    .collect::<HashMap<&'a Function, Value>>()
            })
            .collect::<Vec<HashMap<&'a Function, Value>>>()[0]
            .clone()
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
