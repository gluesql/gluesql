use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use thiserror::Error;

use sqlparser::{
    ast::{ColumnDef, ColumnOption, ColumnOptionDef, Expr, Ident, Value as AstValue},
    dialect::keywords::Keyword,
    tokenizer::{Token, Word},
};

use crate::data::Value;
use crate::result::Result;

#[derive(Error, Serialize, Debug, PartialEq)]
pub enum RowError {
    #[error("lack of required column: {0}")]
    LackOfRequiredColumn(String),

    #[error("literals does not fit to columns")]
    LackOfRequiredValue(String),

    #[error("literals have more values than target columns")]
    TooManyValues,

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

    pub fn new(column_defs: &[ColumnDef], columns: &[Ident], values: &[Expr]) -> Result<Self> {
        if values.len() > column_defs.len() {
            return Err(RowError::TooManyValues.into());
        }

        column_defs
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
                    0 => Some(i),
                    _ => columns.iter().position(|target| target.value == name),
                };

                let default = options
                    .iter()
                    .filter_map(|ColumnOptionDef { option, .. }| match option {
                        ColumnOption::Default(expr) => Some(expr),
                        _ => None,
                    })
                    .next();

                let mut nullable = options
                    .iter()
                    .any(|ColumnOptionDef { option, .. }| option == &ColumnOption::Null);

                let generates = options.iter().any(|ColumnOptionDef { option, .. }| {
                    matches!(option, ColumnOption::DialectSpecific(tokens)
                    if match tokens[..] {
                        [Token::Word(Word {
                            keyword: Keyword::AUTO_INCREMENT,
                            ..
                        }), ..]
                        | [Token::Word(Word {
                            keyword: Keyword::AUTOINCREMENT,
                            ..
                        }), ..] => true, // Doubled due to OR in paterns being experimental; TODO: keyword: Keyword::AUTO_INCREMENT | Keyword::AUTOINCREMENT
                        _ => false,
                    })
                });

                let empty_expr = Expr::Value(AstValue::Null);

                let expr = match (i, default) {
                    (Some(i), _) => values
                        .get(i)
                        .ok_or_else(|| RowError::LackOfRequiredValue(name.clone())),
                    (None, Some(expr)) => Ok(expr),
                    (None, None) => {
                        if
                        /*nullable ||*/
                        generates {
                            // TODO: Do this a better way
                            nullable = true;
                            Ok(&empty_expr)
                        } else {
                            Err(RowError::LackOfRequiredColumn(name.clone()))
                        }
                    }
                }?;

                Value::from_expr(&data_type, nullable, expr)
            })
            .collect::<Result<_>>()
            .map(Self)
    }
}
