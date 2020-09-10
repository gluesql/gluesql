use im_rc::HashMap;
use std::cmp::Ordering;

use sqlparser::ast::Function;

use crate::data::Value;
use crate::result::Result;

pub struct State<'a> {
    index: usize,
    values: HashMap<&'a Function, (usize, Value)>,
}

impl<'a> State<'a> {
    pub fn new() -> Self {
        State {
            index: 0,
            values: HashMap::new(),
        }
    }

    pub fn apply(self, index: usize) -> Self {
        Self {
            index,
            values: self.values,
        }
    }

    pub fn export(self) -> HashMap<&'a Function, Value> {
        self.values
            .iter()
            .map(|(key, (_, value))| (*key, value.clone()))
            .collect()
    }

    pub fn add(self, key: &'a Function, target: &Value) -> Result<Self> {
        let value = match self.values.get(key) {
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
        if let Some((index, value)) = self.values.get(key) {
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
        if let Some((index, value)) = self.values.get(key) {
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

    fn update(self, key: &'a Function, value: Value) -> Self {
        Self {
            index: self.index,
            values: self.values.update(key, (self.index, value)),
        }
    }
}
