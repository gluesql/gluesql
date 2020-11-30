#![cfg(feature = "alter-table")]

use async_trait::async_trait;
use boolinator::Boolinator;
use std::iter::once;
use std::str;

use sqlparser::ast::{ColumnDef, ColumnOption, ColumnOptionDef, Ident, Value as AstValue};

use super::{error::err_into, fetch_schema, SledStorage};
use crate::utils::Vector;
use crate::{AlterTable, AlterTableError, MutResult, Row, Schema, Value};

macro_rules! try_self {
    ($self: expr, $expr: expr) => {
        match $expr {
            Err(e) => {
                return Err(($self, e.into()));
            }
            Ok(v) => v,
        }
    };
}

macro_rules! try_into {
    ($self: expr, $expr: expr) => {
        try_self!($self, $expr.map_err(err_into))
    };
}

macro_rules! fetch_schema {
    ($self: expr, $tree: expr, $table_name: expr) => {{
        let (key, schema) = try_self!($self, fetch_schema($tree, $table_name));
        let schema = try_into!(
            $self,
            schema.ok_or_else(|| AlterTableError::TableNotFound($table_name.to_string()))
        );

        (key, schema)
    }};
}

#[async_trait(?Send)]
impl AlterTable for SledStorage {
    async fn rename_schema(self, table_name: &str, new_table_name: &str) -> MutResult<Self, ()> {
        let (_, Schema { column_defs, .. }) = fetch_schema!(self, &self.tree, table_name);
        let schema = Schema {
            table_name: new_table_name.to_string(),
            column_defs,
        };

        let tree = &self.tree;

        // remove existing schema
        let key = format!("schema/{}", table_name);
        try_into!(self, tree.remove(key));

        // insert new schema
        let value = try_into!(self, bincode::serialize(&schema));
        let key = format!("schema/{}", new_table_name);
        let key = key.as_bytes();
        try_into!(self, self.tree.insert(key, value));

        // replace data
        let prefix = format!("data/{}/", table_name);

        for item in tree.scan_prefix(prefix.as_bytes()) {
            let (key, value) = try_into!(self, item);

            let new_key = try_into!(self, str::from_utf8(key.as_ref()));
            let new_key = new_key.replace(table_name, new_table_name);
            try_into!(self, tree.insert(new_key, value));

            try_into!(self, tree.remove(key));
        }

        Ok((self, ()))
    }

    async fn rename_column(
        self,
        table_name: &str,
        old_column_name: &str,
        new_column_name: &str,
    ) -> MutResult<Self, ()> {
        let (key, Schema { column_defs, .. }) = fetch_schema!(self, &self.tree, table_name);

        let i = column_defs
            .iter()
            .position(|column_def| column_def.name.value == old_column_name)
            .ok_or(AlterTableError::RenamingColumnNotFound);
        let i = try_into!(self, i);

        let ColumnDef {
            name: Ident { quote_style, .. },
            data_type,
            collation,
            options,
        } = column_defs[i].clone();

        let column_def = ColumnDef {
            name: Ident {
                quote_style,
                value: new_column_name.to_string(),
            },
            data_type,
            collation,
            options,
        };
        let column_defs = Vector::from(column_defs).update(i, column_def).into();

        let schema = Schema {
            table_name: table_name.to_string(),
            column_defs,
        };
        let value = try_into!(self, bincode::serialize(&schema));
        try_into!(self, self.tree.insert(key, value));

        Ok((self, ()))
    }

    async fn add_column(self, table_name: &str, column_def: &ColumnDef) -> MutResult<Self, ()> {
        let (
            key,
            Schema {
                table_name,
                column_defs,
            },
        ) = fetch_schema!(self, &self.tree, table_name);

        if column_defs
            .iter()
            .any(|ColumnDef { name, .. }| name.value == column_def.name.value)
        {
            let adding_column = column_def.name.value.to_string();

            return Err((
                self,
                AlterTableError::AddingColumnAlreadyExists(adding_column).into(),
            ));
        }

        let ColumnDef {
            options, data_type, ..
        } = column_def;

        let nullable = options
            .iter()
            .any(|ColumnOptionDef { option, .. }| option == &ColumnOption::Null);
        let default = options
            .iter()
            .filter_map(|ColumnOptionDef { option, .. }| match option {
                ColumnOption::Default(expr) => Some(expr),
                _ => None,
            })
            .map(|expr| Value::from_expr(&data_type, nullable, expr))
            .next();

        let value = match (default, nullable) {
            (Some(value), _) => try_self!(self, value),
            (None, true) => try_self!(
                self,
                Value::from_data_type(&data_type, nullable, &AstValue::Null)
            ),
            (None, false) => {
                return Err((
                    self,
                    AlterTableError::DefaultValueRequired(column_def.to_string()).into(),
                ));
            }
        };

        // migrate data
        let prefix = format!("data/{}/", table_name);

        for item in self.tree.scan_prefix(prefix.as_bytes()) {
            let (key, row) = try_into!(self, item);
            let row: Row = try_into!(self, bincode::deserialize(&row));
            let row = Row(row.0.into_iter().chain(once(value.clone())).collect());
            let row = try_into!(self, bincode::serialize(&row));

            try_into!(self, self.tree.insert(key, row));
        }

        // update schema
        let column_defs = column_defs
            .into_iter()
            .chain(once(column_def.clone()))
            .collect::<Vec<ColumnDef>>();

        let schema = Schema {
            table_name,
            column_defs,
        };
        let schema_value = try_into!(self, bincode::serialize(&schema));
        try_into!(self, self.tree.insert(key, schema_value));

        Ok((self, ()))
    }

    async fn drop_column(
        self,
        table_name: &str,
        column_name: &str,
        if_exists: bool,
    ) -> MutResult<Self, ()> {
        let (
            key,
            Schema {
                table_name,
                column_defs,
            },
        ) = fetch_schema!(self, &self.tree, table_name);

        let index = column_defs
            .iter()
            .position(|ColumnDef { name, .. }| name.value == column_name);

        let index = match (index, if_exists) {
            (Some(index), _) => index,
            (None, true) => {
                return Ok((self, ()));
            }
            (None, false) => {
                return Err((
                    self,
                    AlterTableError::DroppingColumnNotFound(column_name.to_string()).into(),
                ));
            }
        };

        // migrate data
        let prefix = format!("data/{}/", table_name);

        for item in self.tree.scan_prefix(prefix.as_bytes()) {
            let (key, row) = try_into!(self, item);
            let row: Row = try_into!(self, bincode::deserialize(&row));
            let row = Row(row
                .0
                .into_iter()
                .enumerate()
                .filter_map(|(i, v)| (i != index).as_some(v))
                .collect());
            let row = try_into!(self, bincode::serialize(&row));

            try_into!(self, self.tree.insert(key, row));
        }

        // update schema
        let column_defs = column_defs
            .into_iter()
            .enumerate()
            .filter_map(|(i, v)| (i != index).as_some(v))
            .collect::<Vec<ColumnDef>>();

        let schema = Schema {
            table_name,
            column_defs,
        };
        let schema_value = try_into!(self, bincode::serialize(&schema));
        try_into!(self, self.tree.insert(key, schema_value));

        Ok((self, ()))
    }
}
