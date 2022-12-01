use {
    crate::{
        ast::{ColumnDef, Expr},
        data::Value,
        executor::evaluate_stateless,
        result::Result,
    },
    serde::Serialize,
    std::{fmt::Debug, rc::Rc},
    thiserror::Error,
};

#[derive(Error, Serialize, Debug, PartialEq)]
pub enum RowError {
    #[error("lack of required column: {0}")]
    LackOfRequiredColumn(String),

    #[error("wrong column name: {0}")]
    WrongColumnName(String),

    #[error("column and values not matched")]
    ColumnAndValuesNotMatched,

    #[error("literals have more values than target columns")]
    TooManyValues,

    #[error("conflict! row cannot be empty")]
    ConflictOnEmptyRow,

    #[error("VALUES lists must all be the same length")]
    NumberOfValuesDifferent,
}

#[derive(iter_enum::Iterator)]
enum Columns<I1, I2> {
    All(I1),
    Specified(I2),
}

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

    pub fn take_first_value(self) -> Result<Value> {
        self.values
            .into_iter()
            .next()
            .ok_or_else(|| RowError::ConflictOnEmptyRow.into())
    }

    pub fn new(
        column_defs: &[ColumnDef],
        labels: Rc<[String]>,
        columns: &[String],
        values: &[Expr],
    ) -> Result<Self> {
        if !columns.is_empty() && values.len() != columns.len() {
            return Err(RowError::ColumnAndValuesNotMatched.into());
        } else if values.len() > column_defs.len() {
            return Err(RowError::TooManyValues.into());
        }

        if let Some(wrong_column_name) = columns.iter().find(|column_name| {
            !column_defs
                .iter()
                .any(|column_def| &&column_def.name == column_name)
        }) {
            return Err(RowError::WrongColumnName(wrong_column_name.to_owned()).into());
        }

        let columns = if columns.is_empty() {
            Columns::All(column_defs.iter().map(|ColumnDef { name, .. }| name))
        } else {
            Columns::Specified(columns.iter())
        };

        let column_name_value_list = columns.zip(values.iter()).collect::<Vec<(_, _)>>();

        let values = column_defs
            .iter()
            .map(|column_def| {
                let ColumnDef {
                    name: def_name,
                    data_type,
                    nullable,
                    ..
                } = column_def;

                let value = column_name_value_list
                    .iter()
                    .find(|(name, _)| name == &def_name)
                    .map(|(_, value)| value);

                match (value, column_def.get_default(), nullable) {
                    (Some(&expr), _, _) | (None, Some(expr), _) => {
                        evaluate_stateless(None, expr)?.try_into_value(data_type, *nullable)
                    }
                    (None, None, true) => Ok(Value::Null),
                    (None, None, false) => {
                        Err(RowError::LackOfRequiredColumn(def_name.to_owned()).into())
                    }
                }
            })
            .collect::<Result<Vec<Value>>>()?;

        Ok(Row {
            columns: labels,
            values,
        })
    }

    pub fn validate(&self, column_defs: &[ColumnDef]) -> Result<()> {
        let items = column_defs
            .iter()
            .enumerate()
            .filter_map(|(index, column_def)| {
                let value = self.get_value_by_index(index);

                value.map(|v| (v, column_def))
            });

        for (value, column_def) in items {
            let ColumnDef {
                data_type,
                nullable,
                ..
            } = column_def;

            value.validate_type(data_type)?;
            value.validate_null(*nullable)?;
        }

        Ok(())
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
