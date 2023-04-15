use {
    crate::{data::Value, executor::RowContext, result::Result},
    serde::Serialize,
    std::{collections::HashMap, fmt::Debug, rc::Rc},
    thiserror::Error,
};

#[derive(Error, Serialize, Debug, PartialEq, Eq)]
pub enum RowError {
    #[error("conflict - vec expected but map row found")]
    ConflictOnUnexpectedMapRowFound,

    #[error("conflict - map expected but vec row found")]
    ConflictOnUnexpectedVecRowFound,
}

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

    pub fn try_into_vec(self) -> Result<Vec<Value>> {
        match self {
            Self::Vec { values, .. } => Ok(values),
            Self::Map(_) => Err(RowError::ConflictOnUnexpectedMapRowFound.into()),
        }
    }

    pub fn try_into_map(self) -> Result<HashMap<String, Value>> {
        match self {
            Self::Vec { .. } => Err(RowError::ConflictOnUnexpectedVecRowFound.into()),
            Self::Map(values) => Ok(values),
        }
    }

    pub fn as_context(&self) -> RowContext<'_> {
        match self {
            Self::Vec { columns, values } => RowContext::RefVecData { columns, values },
            Self::Map(values) => RowContext::RefMapData(values),
        }
    }
}
