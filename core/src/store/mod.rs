use chrono::NaiveDateTime;
use serde::{Deserialize, Serialize};

use crate::{
    ast::ColumnDef,
    prelude::{DataType, Value},
};

mod alter_table;
mod data_row;
mod index;
mod transaction;

pub trait GStore: Store + Index + Metadata {}
impl<S: Store + Index + Metadata> GStore for S {}

pub trait GStoreMut: StoreMut + IndexMut + AlterTable + Transaction {}
impl<S: StoreMut + IndexMut + AlterTable + Transaction> GStoreMut for S {}

pub use {
    alter_table::{AlterTable, AlterTableError},
    data_row::DataRow,
    index::{Index, IndexError, IndexMut},
    transaction::Transaction,
};

use {
    crate::{
        data::{Key, Schema},
        result::Result,
    },
    async_trait::async_trait,
    serde::{Deserialize, Serialize},
    strum_macros::Display,
};

pub type RowIter = Box<dyn Iterator<Item = Result<(Key, DataRow)>>>;

/// By implementing `Store` trait, you can run `SELECT` query.
#[async_trait(?Send)]
pub trait Store {
    async fn fetch_schema(&self, table_name: &str) -> Result<Option<Schema>>;

    async fn fetch_all_schemas(&self) -> Result<Vec<Schema>>;

    async fn fetch_data(&self, table_name: &str, key: &Key) -> Result<Option<DataRow>>;

    async fn scan_data(&self, table_name: &str) -> Result<RowIter>;
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Meta {
    pub name: MetaName,
    pub row: MetaRow,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, Display)]
#[strum(serialize_all = "SCREAMING_SNAKE_CASE")]
pub enum MetaName {
    GlueObjects,
    GlueTables,
    GlueDatabases,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, Display)]
pub enum MetaRow {
    GlueObjects(GlueObjects),
    GlueTables(GlueTables),
    GlueDatabases(GlueDatabases),
}

impl MetaRow {
    pub fn to_values(self) -> DataRow {
        match self {
            MetaRow::GlueObjects(glue_object) => glue_object.to_values(),
            MetaRow::GlueTables(glue_tables) => todo!(),
            MetaRow::GlueDatabases(glue_databases) => todo!(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct GlueObjects {
    object_name: String,
    object_type: String,
    created: NaiveDateTime,
    last_ddl_time: NaiveDateTime,
}

impl GlueObjects {
    pub fn new(
        object_name: String,
        object_type: String,
        created: NaiveDateTime,
        last_ddl_time: NaiveDateTime,
    ) -> Self {
        Self {
            object_name,
            object_type,
            created,
            last_ddl_time,
        }
    }

    fn to_values(self) -> DataRow {
        let row = vec![
            Value::Str(self.object_name),
            Value::Str(self.object_type),
            Value::Timestamp(self.created),
            Value::Timestamp(self.last_ddl_time),
        ];

        DataRow::Vec(row)
    }

    pub fn to_schema() -> Schema {
        Schema {
            table_name: "GLUE_OBJECTS".to_string(),
            column_defs: Some(vec![
                ColumnDef {
                    name: "object_name".to_string(),
                    data_type: DataType::Text,
                    nullable: false,
                    default: None,
                    unique: None,
                },
                ColumnDef {
                    name: "object_type".to_string(),
                    data_type: DataType::Text,
                    nullable: false,
                    default: None,
                    unique: None,
                },
                ColumnDef {
                    name: "created".to_string(),
                    data_type: DataType::Timestamp,
                    nullable: false,
                    default: None,
                    unique: None,
                },
                ColumnDef {
                    name: "last_ddl_time".to_string(),
                    data_type: DataType::Timestamp,
                    nullable: false,
                    default: None,
                    unique: None,
                },
            ]),
            indexes: Vec::new(),
            engine: None,
            created: NaiveDateTime::default(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct GlueTables {
    table_name: String,
    engine: String,
    has_schema: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct GlueDatabases {
    created: bool,
    startup_time: bool,
    engine: bool,
    version: bool,
    open_mode: bool,
}

#[async_trait(?Send)]
pub trait Metadata {
    async fn scan_meta(&self, meta: &MetaName) -> Result<RowIter> {
        unimplemented!("unimplemented scan_meta");
    }

    async fn append_meta(&mut self, meta: Meta) -> Result<()> {
        unimplemented!("unimplemented append_meta");
    }

    async fn insert_meta(&mut self, meta: &Meta, rows: Vec<(Key, DataRow)>) -> Result<()> {
        unimplemented!("unimplemented insert_meta");
    }

    async fn delete_meta(&mut self, meta: &Meta, keys: Vec<Key>) -> Result<()> {
        unimplemented!("unimplemented delete_meta");
    }
}

/// By implementing `StoreMut` trait,
/// you can run `INSERT`, `CREATE TABLE`, `DELETE`, `UPDATE` and `DROP TABLE` queries.
#[async_trait(?Send)]
pub trait StoreMut {
    async fn insert_schema(&mut self, schema: &Schema) -> Result<()>;

    async fn delete_schema(&mut self, table_name: &str) -> Result<()>;

    async fn append_data(&mut self, table_name: &str, rows: Vec<DataRow>) -> Result<()>;

    async fn insert_data(&mut self, table_name: &str, rows: Vec<(Key, DataRow)>) -> Result<()>;

    async fn delete_data(&mut self, table_name: &str, keys: Vec<Key>) -> Result<()>;
}
