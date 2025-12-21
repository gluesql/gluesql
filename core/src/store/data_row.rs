use {
    crate::{
        data::{Row, Value},
        executor::RowContext,
    },
    serde::{Deserialize, Serialize},
};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DataRow(pub Vec<Value>);

impl From<Vec<Value>> for DataRow {
    fn from(values: Vec<Value>) -> Self {
        Self(values)
    }
}

impl From<Row> for DataRow {
    fn from(row: Row) -> Self {
        Self(row.values)
    }
}

impl DataRow {
    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn as_context<'a>(&'a self, columns: Option<&'a [String]>) -> RowContext<'a> {
        RowContext::RefVecData {
            columns: columns.unwrap_or(&[]),
            values: &self.0,
        }
    }

    pub fn into_values(self) -> Vec<Value> {
        self.0
    }
}
