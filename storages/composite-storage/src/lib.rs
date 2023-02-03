#![deny(clippy::str_to_string)]

mod store;
mod store_mut;
mod transaction;

use {
    gluesql_core::{
        data::Schema,
        result::{Error, Result},
        store::{GStore, GStoreMut, Store},
    },
    std::collections::HashMap,
};

pub trait IStorage: GStore + GStoreMut {}

impl<T: GStore + GStoreMut> IStorage for T {}

#[derive(Default)]
pub struct CompositeStorage {
    pub storages: HashMap<String, Box<dyn IStorage>>,
    pub default_engine: Option<String>,
}

impl<T: GStore + GStoreMut + 'static> From<T> for Box<dyn IStorage> {
    fn from(storage: T) -> Self {
        Box::new(storage) as Box<dyn IStorage>
    }
}

impl CompositeStorage {
    pub fn new() -> Self {
        CompositeStorage::default()
    }

    pub fn set_default<T: Into<String>>(&mut self, default_engine: T) {
        self.default_engine = Some(default_engine.into());
    }

    pub fn remove_default(&mut self) {
        self.default_engine = None;
    }

    pub fn push<T: Into<String>, U: Into<Box<dyn IStorage>>>(&mut self, engine: T, storage: U) {
        self.storages.insert(engine.into(), storage.into());
    }

    pub fn remove<T: AsRef<str>>(&mut self, engine: T) -> Option<Box<dyn IStorage>> {
        let engine = engine.as_ref();

        if self.default_engine.as_deref() == Some(engine) {
            self.default_engine = None;
        }

        self.storages.remove(engine)
    }

    pub fn clear(&mut self) {
        self.storages.clear();
        self.default_engine = None;
    }

    async fn fetch_engine(&self, table_name: &str) -> Result<String> {
        self.fetch_schema(table_name)
            .await?
            .and_then(|Schema { engine, .. }| engine)
            .or_else(|| self.default_engine.clone())
            .ok_or_else(|| Error::StorageMsg(format!("engine not found for table: {table_name}")))
    }

    async fn fetch_storage(&self, table_name: &str) -> Result<Option<&Box<dyn IStorage>>> {
        self.fetch_engine(table_name)
            .await
            .map(|engine| self.storages.get(&engine))
    }

    async fn fetch_storage_mut(
        &mut self,
        table_name: &str,
    ) -> Result<Option<&mut Box<dyn IStorage>>> {
        self.fetch_engine(table_name)
            .await
            .map(|engine| self.storages.get_mut(&engine))
    }
}

#[cfg(feature = "alter-table")]
impl gluesql_core::store::AlterTable for CompositeStorage {}
#[cfg(feature = "index")]
impl gluesql_core::store::Index for CompositeStorage {}
#[cfg(feature = "index")]
impl gluesql_core::store::IndexMut for CompositeStorage {}
