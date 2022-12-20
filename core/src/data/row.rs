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
            Self::Map(values) => Some(values.get(ident).unwrap_or(&Value::Null)),
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = (&String, &Value)> {
        #[derive(iter_enum::Iterator)]
        enum Entries<I1, I2> {
            Vec(I1),
            Map(I2),
        }

        match self {
            Self::Vec { columns, values } => Entries::Vec(columns.iter().zip(values.iter())),
            Self::Map(values) => Entries::Map(values.iter()),
        }
    }

    // temp
    pub fn into_values(self) -> Vec<Value> {
        match self {
            Self::Vec { values, .. } => values,
            Self::Map(_) => todo!(),
        }
    }
}
