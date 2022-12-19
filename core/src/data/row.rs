use {
    crate::data::Value,
    std::{collections::HashMap, fmt::Debug, rc::Rc},
};

#[derive(Clone, Debug, PartialEq)]
pub enum Row {
    Vec {
        columns: Rc<[String]>,
        values: Vec<Value>,
    },
    Map(HashMap<String, Value>),
}

impl Row {
    pub fn get_value(&self, ident: &str) -> Option<&Value> {
        match self {
            Self::Vec { columns, values } => columns
                .iter()
                .position(|column| column == ident)
                .and_then(|index| values.get(index)),
            Self::Map(values) => values.get(ident),
        }
    }

    // temp
    pub fn into_values(self) -> Vec<Value> {
        match self {
            Self::Vec { values, .. } => values,
            Self::Map(_) => todo!(),
        }
    }

    // temp
    pub fn get_values(&self) -> &[Value] {
        match self {
            Self::Vec { values, .. } => values.as_slice(),
            Self::Map(_) => todo!(),
        }
    }
}
