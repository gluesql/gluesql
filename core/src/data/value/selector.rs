use {
    super::{Value, ValueError},
    crate::result::Result,
    std::{
        collections::HashMap,
        ops::ControlFlow::{Break, Continue},
    },
};

enum Selectable<'a> {
    Map(&'a HashMap<String, Value>),
    List(&'a Vec<Value>),
    Other(&'a Value),
}

impl Value {
    pub fn selector(&self, selector: &str) -> Result<Value> {
        let selectable = match self {
            Value::Map(v) => Selectable::Map(v),
            Value::List(v) => Selectable::List(v),
            _ => return Err(ValueError::SelectorRequiresMapOrListTypes.into()),
        };

        let result = selector.split('.').try_fold(selectable, |selectable, key| {
            let value = match selectable {
                Selectable::Map(map) => map.get(key),
                Selectable::List(list) => key.parse::<usize>().ok().and_then(|i| list.get(i)),
                Selectable::Other(_) => return Break(()),
            };

            match value {
                Some(Value::Map(map)) => Continue(Selectable::Map(map)),
                Some(Value::List(list)) => Continue(Selectable::List(list)),
                Some(value) => Continue(Selectable::Other(value)),
                None => Break(()),
            }
        });

        let value = match result {
            Continue(Selectable::Map(map)) => Value::Map(map.clone()),
            Continue(Selectable::List(list)) => Value::List(list.clone()),
            Continue(Selectable::Other(value)) => value.clone(),
            Break(_) => Value::Null,
        };

        Ok(value)
    }

    pub fn selector_by_index(&self, selector: &[Value]) -> Result<Value> {
        selector
            .iter()
            .map(String::from)
            .try_fold(self, |selectable, key| {
                selectable.get_value_from_compound_type(&key)
            })
            .cloned()
    }

    fn get_value_from_compound_type(&self, key: &str) -> Result<&Value> {
        let value = match self {
            Value::Map(map) => map.get(key),
            Value::List(list) => key.parse::<usize>().ok().and_then(|i| list.get(i)),
            _ => return Err(ValueError::SelectorRequiresMapOrListTypes.into()),
        };

        Ok(value.unwrap_or(&Value::Null))
    }
}
