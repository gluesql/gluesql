use {
    crate::data::{Row, Value},
    serde::{Deserialize, Serialize},
    std::collections::HashMap,
};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum DataRow {
    Vec(Vec<Value>),
    Map(HashMap<String, Value>),
}

impl From<Row> for DataRow {
    fn from(row: Row) -> Self {
        match row {
            Row::Vec { values, .. } => Self::Vec(values),
            Row::Map(values) => Self::Map(values),
        }
    }
}

impl From<Vec<Value>> for DataRow {
    fn from(values: Vec<Value>) -> Self {
        Self::Vec(values)
    }
}

impl DataRow {
    pub fn len(&self) -> usize {
        match self {
            Self::Vec(values) => values.len(),
            Self::Map(values) => values.len(),
        }
    }
}
