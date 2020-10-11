use sled::{self, Config, Db, IVec};
use std::convert::TryFrom;
use std::iter::once;
use std::str;
use thiserror::Error as ThisError;

use sqlparser::ast::{ColumnDef, ColumnOption, ColumnOptionDef, Ident, Value as AstValue};

use crate::utils::Vector;
use crate::{
    AlterTable, AlterTableError, Error, MutResult, Result, Row, RowIter, Schema, Store, StoreError,
    StoreMut, Value,
};

#[derive(ThisError, Debug)]
enum StorageError {
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    AlterTable(#[from] AlterTableError),

    #[error(transparent)]
    Sled(#[from] sled::Error),
    #[error(transparent)]
    Bincode(#[from] bincode::Error),
    #[error(transparent)]
    Str(#[from] str::Utf8Error),
}

impl Into<Error> for StorageError {
    fn into(self) -> Error {
        use StorageError::*;

        match self {
            Sled(e) => Error::Storage(Box::new(e)),
            Bincode(e) => Error::Storage(e),
            Str(e) => Error::Storage(Box::new(e)),
            Store(e) => e.into(),
            AlterTable(e) => e.into(),
        }
    }
}

macro_rules! try_into {
    ($expr: expr) => {
        $expr.map_err(|e| {
            let e: StorageError = e.into();
            let e: Error = e.into();

            e
        })?
    };
    ($self: expr, $expr: expr) => {
        match $expr {
            Err(e) => {
                let e: StorageError = e.into();
                let e: Error = e.into();

                return Err(($self, e));
            }
            Ok(v) => v,
        }
    };
}

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

#[derive(Debug)]
pub struct SledStorage {
    tree: Db,
}

impl SledStorage {
    pub fn new(filename: &str) -> Result<Self> {
        let tree = try_into!(sled::open(filename));

        Ok(Self { tree })
    }
}

impl TryFrom<Config> for SledStorage {
    type Error = Error;

    fn try_from(config: Config) -> Result<Self> {
        let tree = try_into!(config.open());

        Ok(Self { tree })
    }
}

impl StoreMut<IVec> for SledStorage {
    fn generate_id(self, table_name: &str) -> MutResult<Self, IVec> {
        let id = try_into!(self, self.tree.generate_id());
        let id = format!("data/{}/{}", table_name, id);

        Ok((self, IVec::from(id.as_bytes())))
    }

    fn insert_schema(self, schema: &Schema) -> MutResult<Self, ()> {
        let key = format!("schema/{}", schema.table_name);
        let key = key.as_bytes();
        let value = try_into!(self, bincode::serialize(schema));

        try_into!(self, self.tree.insert(key, value));

        Ok((self, ()))
    }

    fn delete_schema(self, table_name: &str) -> MutResult<Self, ()> {
        let prefix = format!("data/{}/", table_name);
        let tree = &self.tree;

        for item in tree.scan_prefix(prefix.as_bytes()) {
            let (key, _) = try_into!(self, item);

            try_into!(self, tree.remove(key));
        }

        let key = format!("schema/{}", table_name);
        try_into!(self, tree.remove(key));

        Ok((self, ()))
    }

    fn insert_data(self, key: &IVec, row: Row) -> MutResult<Self, ()> {
        let value = try_into!(self, bincode::serialize(&row));

        try_into!(self, self.tree.insert(key, value));

        Ok((self, ()))
    }

    fn delete_data(self, key: &IVec) -> MutResult<Self, ()> {
        try_into!(self, self.tree.remove(key));

        Ok((self, ()))
    }
}

impl Store<IVec> for SledStorage {
    fn fetch_schema(&self, table_name: &str) -> Result<Schema> {
        fetch_schema(&self.tree, table_name).map(|(_, schema)| schema)
    }

    fn scan_data(&self, table_name: &str) -> Result<RowIter<IVec>> {
        let prefix = format!("data/{}/", table_name);

        let result_set = self.tree.scan_prefix(prefix.as_bytes()).map(move |item| {
            let (key, value) = try_into!(item);
            let value = try_into!(bincode::deserialize(&value));

            Ok((key, value))
        });

        Ok(Box::new(result_set))
    }
}

impl AlterTable for SledStorage {
    fn rename_schema(self, table_name: &str, new_table_name: &str) -> MutResult<Self, ()> {
        let (_, Schema { column_defs, .. }) = try_self!(self, fetch_schema(&self.tree, table_name));

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

    fn rename_column(
        self,
        table_name: &str,
        old_column_name: &str,
        new_column_name: &str,
    ) -> MutResult<Self, ()> {
        let (key, Schema { column_defs, .. }) =
            try_self!(self, fetch_schema(&self.tree, table_name));

        let i = column_defs
            .iter()
            .position(|column_def| column_def.name.value == old_column_name)
            .ok_or(AlterTableError::ColumnNotFound);
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

    fn add_column(self, table_name: &str, column_def: &ColumnDef) -> MutResult<Self, ()> {
        let (
            key,
            Schema {
                table_name,
                column_defs,
            },
        ) = try_self!(self, fetch_schema(&self.tree, table_name));

        if column_defs
            .iter()
            .any(|ColumnDef { name, .. }| name.value == column_def.name.value)
        {
            return Err((
                self,
                AlterTableError::ColumnAlreadyExists(column_def.name.value.to_string()).into(),
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
}

fn fetch_schema(tree: &Db, table_name: &str) -> Result<(String, Schema)> {
    let key = format!("schema/{}", table_name);
    let value = try_into!(tree.get(&key.as_bytes()));
    let value = value.ok_or(StoreError::SchemaNotFound)?;
    let schema = try_into!(bincode::deserialize(&value));

    Ok((key, schema))
}
