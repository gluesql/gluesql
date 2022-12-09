use {
    crate::data::Value,
    std::{fmt::Debug, rc::Rc},
};

#[derive(Clone, Debug, PartialEq)]
pub struct Row {
    pub columns: Rc<[String]>,
    pub values: Vec<Value>,
}

impl Row {
    pub fn get_value_by_index(&self, index: usize) -> Option<&Value> {
        self.values.get(index)
    }

    pub fn get_value(&self, ident: &str) -> Option<&Value> {
        self.columns
            .iter()
            .position(|column| column == ident)
            .and_then(|index| self.values.get(index))
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }
}

impl From<Row> for Vec<Value> {
    fn from(row: Row) -> Self {
        row.values
    }
}

#[cfg(test)]
mod tests {
    use {super::Row, crate::data::Value, std::rc::Rc};

    #[test]
    fn len() {
        let row = Row {
            columns: Rc::from(vec!["T".to_owned()]),
            values: vec![Value::Bool(true), Value::I64(100)],
        };

        assert_eq!(row.len(), 2);
        assert!(!row.is_empty());
    }
}
