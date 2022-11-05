use {
    crate::{
        ast::{ColumnDef, Expr},
        data::Value,
        executor::evaluate_stateless,
        result::Result,
    },
    serde::{Deserialize, Serialize},
    std::{fmt::Debug, slice::Iter, vec::IntoIter},
    thiserror::Error,
};

#[derive(Error, Serialize, Debug, PartialEq)]
pub enum RowError {
    #[error("lack of required column: {0}")]
    LackOfRequiredColumn(String),

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

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Row(pub Vec<Value>);

impl Row {
    pub fn get_value_by_index(&self, index: usize) -> Option<&Value> {
        self.0.get(index)
    }

    pub fn get_value(&self, columns: &[String], ident: &str) -> Option<&Value> {
        columns
            .iter()
            .position(|column| column == ident)
            .and_then(|index| self.0.get(index))
    }

    pub fn take_first_value(self) -> Result<Value> {
        self.0
            .into_iter()
            .next()
            .ok_or_else(|| RowError::ConflictOnEmptyRow.into())
    }

    pub fn new(column_defs: &[ColumnDef], columns: &[String], values: &[Expr]) -> Result<Self> {
        if !columns.is_empty() && values.len() != columns.len() {
            return Err(RowError::ColumnAndValuesNotMatched.into());
        } else if values.len() > column_defs.len() {
            return Err(RowError::TooManyValues.into());
        }

        let columns = if columns.is_empty() {
            Columns::All(column_defs.iter().map(|ColumnDef { name, .. }| name))
        } else {
            Columns::Specified(columns.iter())
        };

        let column_name_value_list = columns.zip(values.iter()).collect::<Vec<(_, _)>>();

        column_defs
            .iter()
            .map(|column_def| {
                let ColumnDef {
                    name: def_name,
                    data_type,
                    ..
                } = column_def;

                let value = column_name_value_list
                    .iter()
                    .find(|(name, _)| name == &def_name)
                    .map(|(_, value)| value);

                let nullable = column_def.is_nullable();

                match (value, column_def.get_default(), nullable) {
                    (Some(&expr), _, _) | (None, Some(expr), _) => {
                        evaluate_stateless(None, expr)?.try_into_value(data_type, nullable)
                    }
                    (None, None, true) => Ok(Value::Null),
                    (None, None, false) => {
                        Err(RowError::LackOfRequiredColumn(def_name.to_owned()).into())
                    }
                }
            })
            .collect::<Result<_>>()
            .map(Self)
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
            let ColumnDef { data_type, .. } = column_def;
            let nullable = column_def.is_nullable();

            value.validate_type(data_type)?;
            value.validate_null(nullable)?;
        }

        Ok(())
    }

    pub fn iter(&self) -> Iter<'_, Value> {
        self.0.iter()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl IntoIterator for Row {
    type Item = Value;
    type IntoIter = IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl From<Row> for Vec<Value> {
    fn from(row: Row) -> Self {
        row.0
    }
}

impl From<Vec<Value>> for Row {
    fn from(values: Vec<Value>) -> Self {
        Row(values)
    }
}

#[cfg(test)]
mod tests {
    use {super::Row, crate::data::Value};

    #[test]
    fn len() {
        let row: Row = vec![Value::Bool(true), Value::I64(100)].into();

        assert_eq!(row.len(), 2);
        assert!(!row.is_empty());
    }
}
