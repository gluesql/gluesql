#![cfg(feature = "index")]

use async_trait::async_trait;
use std::ops::Range;
use std::str;

use sled::IVec;

use super::{error::err_into, SledStorage};
use crate::{Condition, Index, IndexError, MutResult, Result, Row, RowIter, Schema, Value};
use fstrings::*;
use std::fmt::Debug;

#[async_trait(?Send)]
impl Index<IVec> for SledStorage {
    async fn create(
        self,
        table_name: &str,
        row_names: Vec<&str>,
        unique: bool,
    ) -> MutResult<Self, ()> {
        Ok((self, ()))
    }

    async fn drop(self, table_name: &str, row_names: Vec<&str>) -> MutResult<Self, ()> {
        Ok((self, ()))
    }

    async fn get_by_key(&self, table_name: &str, key: IVec) -> Result<Row> {
        let prefix = format!("data/{}/", table_name);
        let prefix = prefix.into_bytes().into_iter();
        let concat = |values: Vec<IVec>| {
            values.into_iter().fold(prefix, |result, value| {
                let value = value
                    .iter()
                    .fold(String::from(""), |result, value| f!("{result}{value}"));
                f!("{result}/{value}")
            })
        };
        let key = concat(key);

        let (_, row) = self.tree.get(key)?;
        let row = bincode::deserialize(&row).map_err(err_into)?;
        Ok(row)
    }

    async fn get_indexed_keys(&self, condition: &Condition, table_name: &str) -> Result<Vec<IVec>> {
        let ((min, max), column_name) = match condition {
            Condition::Equals { column_name, value } => ((Some(value), Some(value)), column_name),
            Condition::GreaterThanOrEquals { column_name, value } => {
                ((Some(value), None), column_name)
            }
            Condition::LessThanOrEquals { column_name, value } => {
                ((None, Some(value)), column_name)
            }
            //GreaterThan => ??,
            //LessThan => ??,
            _ => {
                return Err(
                    IndexError::Unimplemented(String::from("Unimplemented condition")).into(),
                )
            }
        };

        let min = if let Some(value) = min {
            bincode::serialize(&value).ok()
        } else {
            None
        };

        let max = if let Some(value) = max {
            bincode::serialize(&value).ok()
        } else {
            None
        };

        let (min, max): (Vec<Vec<u8>>, Vec<Vec<u8>>) = (
            match min {
                None => vec![vec![0]],
                Some(value) => vec![vec![1], value],
            },
            match max {
                None => vec![vec![2]],
                Some(value) => vec![vec![1], value],
            },
        );
        let prefix = f!("index/{table_name}/{column_name}");
        let concat = |values: Vec<Vec<u8>>| {
            values.into_iter().fold(prefix, |result, value| {
                let value = value
                    .iter()
                    .fold(String::from(""), |result, value| f!("{result}{value}"));
                f!("{result}/{value}")
            })
        };
        let prefix = prefix.into_bytes().into_iter();
        let (min, max) = (concat(min), concat(max));
        let (min, max) = (min.into_bytes().into_iter(), max.into_bytes().into_iter());
        let (min, max): (IVec, IVec) = (prefix.chain(min).collect(), prefix.chain(max).collect());
        Ok(self
            .tree
            .range(Range {
                start: min,
                end: max,
            })
            .filter_map(|item| {
                item.map(|(key, primary_key)| primary_key)
                    .map_err(err_into)
                    .ok()
            })
            .collect::<Vec<IVec>>())
    }
}
