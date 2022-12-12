use {
    crate::data::Value,
    std::{fmt::Debug, rc::Rc, collections::HashMap},
};

/*
#[derive(Clone, Debug, PartialEq)]
pub struct Row {
    pub columns: Rc<[String]>,
    pub values: Vec<Value>,
}
*/

#[derive(Clone, Debug, PartialEq)]
pub enum Row {
    Vec {
        columns: Rc<[String]>,
        values: Vec<Value>,
    },
    Map(HashMap<String, Value>),
}

impl Row {
    pub fn get_value_by_index(&self, index: usize) -> Option<&Value> {
        match self {
            Self::Vec { values, .. } => values.get(index),
            Self::Map(_) => todo!(),
        }

        // self.values.get(index)
    }

    pub fn get_value(&self, ident: &str) -> Option<&Value> {
        match self {
            Self::Vec { columns, values } => {
                columns
                    .iter()
                    .position(|column| column == ident)
                    .and_then(|index| values.get(index))
            }
            Self::Map(_) => todo!(),
        }
        /*
        self.columns
            .iter()
            .position(|column| column == ident)
            .and_then(|index| self.values.get(index))
            */
    }

    pub fn len(&self) -> usize {
        match self {
            Self::Vec { values, .. } => values.len(),
            Self::Map(values) => values.len(),
        }

        // self.values.len()
    }

    pub fn is_empty(&self) -> bool {
        match self {
            Self::Vec { values, .. } => values.is_empty(),
            Self::Map(values) => values.is_empty(),
        }

        // self.values.is_empty()
    }

    // temp
    pub fn into_values(self) -> Vec<Value> {
        match self {
            Self::Vec { values, .. } => values,
            Self::Map(_) => todo!(),
        }
    }

    // temp
    pub fn get_columns(&self) -> Rc<[String]> {
        match self {
            Self::Vec { columns, .. } => Rc::clone(&columns),
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

impl From<Row> for Vec<Value> {
    fn from(row: Row) -> Self {
        match row {
            Row::Vec { values, .. } => values,
            Row::Map(_) => todo!(),
        }

        // row.values
    }
}

#[cfg(test)]
mod tests {
    use {super::Row, crate::data::Value, std::rc::Rc};

    #[test]
    fn len() {
        let row = Row::Vec {
            columns: Rc::from(vec!["T".to_owned()]),
            values: vec![Value::Bool(true), Value::I64(100)],
        };

        assert_eq!(row.len(), 2);
        assert!(!row.is_empty());
    }
}
