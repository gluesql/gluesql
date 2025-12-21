use {
    crate::{data::Value, executor::RowContext},
    std::sync::Arc,
};

#[derive(Clone, Debug, PartialEq)]
pub struct Row {
    pub columns: Arc<[String]>,
    pub values: Vec<Value>,
}

impl Row {
    pub fn get_value(&self, ident: &str) -> Option<&Value> {
        self.columns
            .iter()
            .position(|column| column == ident)
            .and_then(|index| self.values.get(index))
    }

    pub fn iter(&self) -> impl Iterator<Item = (&String, &Value)> {
        self.columns.iter().zip(self.values.iter())
    }

    pub fn into_values(self) -> Vec<Value> {
        self.values
    }

    pub fn as_context(&self) -> RowContext<'_> {
        RowContext::RefVecData {
            columns: &self.columns,
            values: &self.values,
        }
    }
}
