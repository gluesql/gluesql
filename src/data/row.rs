use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use thiserror::Error;

use sqlparser::ast::{
    ColumnDef, ColumnOption, ColumnOptionDef, Expr, Ident, Query, SetExpr, Values,
};

use crate::data::Value;
use crate::result::Result;

#[derive(Error, Serialize, Debug, PartialEq)]
pub enum RowError {
    #[error("lack of required column: {0}")]
    LackOfRequiredColumn(String),

    #[error("literals does not fit to columns")]
    LackOfRequiredValue(String),

    #[error("unsupported ast value type")]
    UnsupportedAstValueType,

    #[error("Unreachable")]
    Unreachable,

    #[error("conflict! row cannot be empty")]
    ConflictOnEmptyRow,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Row(pub Vec<Value>);

impl Row {
    pub fn get_value(&self, index: usize) -> Option<&Value> {
        self.0.get(index)
    }

    pub fn take_first_value(self) -> Result<Value> {
        self.0
            .into_iter()
            .next()
            .ok_or_else(|| RowError::ConflictOnEmptyRow.into())
    }

    pub fn new(
        column_defs: Vec<ColumnDef>,
        columns: &[Ident],
        source: &Query,
    ) -> Result<Vec<Self>> {
        let values = match &source.body {
            SetExpr::Values(Values(values)) => values,
            _ => {
                return Err(RowError::Unreachable.into());
            }
        };

        if values.is_empty() {
            return Err(RowError::Unreachable.into());
        }

        values
            .iter()
            .map(|values| {
                (&column_defs)
                    .iter()
                    .enumerate()
                    .map(|(i, column_def)| {
                        let ColumnDef {
                            name,
                            data_type,
                            options,
                            ..
                        } = column_def;
                        let name = name.to_string();

                        let i = match columns.len() {
                            0 => Ok(i),
                            _ => columns
                                .iter()
                                .position(|target| target.value == name)
                                .ok_or_else(|| RowError::LackOfRequiredColumn(name.clone())),
                        }?;

                        let literal = values
                            .get(i)
                            .ok_or_else(|| RowError::LackOfRequiredValue(name.clone()))?;
                        let nullable = options
                            .iter()
                            .any(|ColumnOptionDef { option, .. }| option == &ColumnOption::Null);

                        match literal {
                            Expr::Value(literal) => {
                                Value::from_data_type(data_type.clone(), nullable, literal)
                            }
                            Expr::Identifier(Ident { value, .. }) => Ok(Value::Str(value.clone())),
                            _ => Err(RowError::UnsupportedAstValueType.into()),
                        }
                    })
                    .collect::<Result<_>>()
                    .map(Self)
            })
            .collect()
    }
}
