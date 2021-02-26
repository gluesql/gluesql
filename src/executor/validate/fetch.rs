use im_rc::HashSet;
use serde::Serialize;
use sqlparser::ast::{ColumnDef, DataType, ColumnOption, Ident};
use std::{convert::TryInto, fmt::Debug, rc::Rc};
use thiserror::Error as ThisError;

use crate::data::{Row, Value};
use crate::result::Result;
use crate::store::Store;
use crate::utils::Vector;

fn fetch_all_unique_columns(column_defs: &[ColumnDef]) -> Vec<(usize, String)> {
    column_defs
        .iter()
        .enumerate()
        .filter_map(|(i, table_col)| {
            if table_col
                .options
                .iter()
                .any(|opt_def| matches!(opt_def.option, ColumnOption::Unique { .. }))
            {
                Some((i, table_col.name.value.to_owned()))
            } else {
                None
            }
        })
        .collect()
}

fn fetch_all_columns_of_type(column_defs: &[ColumnDef], type: DataType) -> Vec<(usize, String)> {
    column_defs
        .iter()
        .enumerate()
        .filter_map(|(i, table_col)| {
            if matches!(table_col.data_type, type) {
                Some((i, table_col.name.value.to_owned()))
            } else {
                None
            }
        })
        .collect()
}

fn fetch_specified_columns_of_type(
    all_column_defs: &[ColumnDef],
    specified_columns: &[Ident],
) -> Vec<(usize, String)> {
    column_defs
        .iter()
        .enumerate()
        .filter_map(|(i, table_col)| {
            if matches!(table_col.data_type, type) && specified_columns.any(|specified_col| specified_col.value == table_col.name.value) {
                Some((i, table_col.name.value.to_owned()))
            } else {
                None
            }
        })
        .collect()
}

// KG: Made this so that code isn't repeated... Perhaps this is inefficient though?
fn specified_columns_only(
    matched_columns: Vec<(usize, String)>,
    specified_columns: &[Ident],
) -> Vec<(usize, String)> {
    matched_columns
        .iter()
        .enumerate()
        .filter_map(|(i, table_col)| {
            if specified_columns.any(|specified_col| specified_col.value == table_col.name.value) {
                Some((i, table_col.name.value.to_owned()))
            } else {
                None
            }
        })
        .collect()
}