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
        // let column_name_value_list = columns.zip(values.iter());
        // let a = column_name_value_list.fold(Vec::new(), |mut acc, cur| {
        //     acc.push(cur);

        //     acc
        // });

        // todo!();

        /*
        A B C
        \ V \

        column_name, column_def.name, value
        Y =>
        N => definately Err
        */
        // column_name_value_list.iter().map(|(name, value)| {
        //     if let Some(column_def) = column_defs
        //         .iter()
        //         .find(|column_def| name == &&column_def.name)
        //     {
        //         let ColumnDef {
        //             name: def_name,
        //             data_type,
        //             nullable,
        //             ..
        //         } = column_def;

        //         return match (column_def.get_default(), nullable) {
        //             (None, true) => Ok(Value::Null),
        //             (None, false) =>
        //             (Some(&expr), _) => todo!(),
        //         };
        //     }
        // });

        column_defs
            .iter()
            .map(|column_def| {
                let ColumnDef {
                    name: def_name,
                    data_type,
                    nullable,
                    ..
                } = column_def;

                /*
                insert into T (B) values(2);
                insert into T (X) values(2);

                A => 1) is A? => value 2) default? default 3) nullable? null 4) check name exists in table
                not found, no default, not null

                */

                let value = column_name_value_list
                    .iter()
                    .find(|(name, _)| name == &def_name)
                    .map(|(_, value)| value);

                // match (value, column_def.get_default()) {
                //     (Some(&expr), _) | (None, Some(expr)) => {
                //         evaluate_stateless(None, expr)?.try_into_value(data_type, *nullable)
                //     }
                //     (None, None) => {
                //         let known_column = column_name_value_list.iter().any(|(name, _)| {
                //             column_defs
                //                 .iter()
                //                 .any(|column_def| &&column_def.name == name)
                //         });

                //         match known_column {
                //             true => Ok(Value::Null),
                //             false => {
                //                 Err(RowError::WrongColumnName(wrong_column_name.to_owned()).into())
                //             }
                //         }
                //     }
                // }

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
