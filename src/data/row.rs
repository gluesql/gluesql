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
use crate::store::{Store, StoreMut};
use futures::executor;

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

    pub fn new<T: 'static + Debug, U: Store<T> + StoreMut<T> + Clone>(
        column_defs: &[ColumnDef],
        columns: &[Ident],
        values: &[Expr],
        table_name: &str,
        storage: U,
    ) -> Result<Self> {
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

                let nullable = options
                    .iter()
                    .any(|ColumnOptionDef { option, .. }| option == &ColumnOption::Null);

                let i = match columns.len() {
                    0 => Some(i),
                    _ => columns.iter().position(|target| target.value == name),
                };

                let generate = options.iter().find_map(|ColumnOptionDef { option, .. }| {
                    match option {
                        ColumnOption::Default(expr) => {
                            Value::from_expr(&data_type, nullable, expr).ok()
                        }
                        #[cfg(feature = "sled-storage")]
                        // Sled only for now. TODO: Non-sled support
                        ColumnOption::DialectSpecific(tokens)
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
                            } =>
                        {
                            let generated = executor::block_on(
                                storage.clone().get_generator(&table_name, &name),
                            )
                            .ok();
                            let value = match generated {
                                Some(Value::I64(value)) => value,
                                _ => 1,
                            };
                            let set = executor::block_on(storage.clone().set_generator(
                                &table_name,
                                &name,
                                Value::I64(value + 1),
                            ));
                            Some(Value::I64(value))
                        }
                        _ => None,
                    }
                });

                match (i, generate) {
                    (Some(i), _) => match values
                        .get(i)
                        .ok_or_else(|| RowError::LackOfRequiredValue(name.clone()))
                    {
                        Ok(expr) => Value::from_expr(&data_type, nullable, expr),
                        Err(e) => Err(e.into()),
                    },
                    (None, Some(value)) => Ok(value),
                    (None, _) => Err(RowError::LackOfRequiredColumn(name.clone()).into()),
                }
            })
            .collect::<Result<_>>()
            .map(Self)
    }
}
