//! Submodule providing the Trigger-related traits and its implementations.
use {
    super::{Store, StoreMut},
    crate::{data::Trigger, result::Result},
    async_trait::async_trait,
};

use serde::Serialize;
use thiserror::Error as ThisError;

#[derive(ThisError, Serialize, Debug, PartialEq, Eq)]
/// Errors that can occur when working with triggers.
pub enum TriggerError {
    #[error("Trigger {trigger_name} associated to table {table_name} does not exist")]
    /// The trigger was not found in the storage associated to the given table.
    TriggerNotFound {
        trigger_name: String,
        table_name: String,
    },
}

#[async_trait(?Send)]
pub trait CustomTrigger {
    /// Fetches a trigger from the storage.
    ///
    /// # Arguments
    /// * `trigger_name` - The name of the trigger to fetch.
    /// * `table_name` - The name of the table the trigger is associated with.
    async fn fetch_trigger(&self, _trigger_name: &str, _table_name: &str) -> Result<Trigger>;
}

#[async_trait(?Send)]
impl<G: Store> CustomTrigger for G {
    async fn fetch_trigger(&self, trigger_name: &str, table_name: &str) -> Result<Trigger> {
        let schema = self.fetch_schema(table_name).await?;
        let schema = schema.ok_or_else(|| TriggerError::TriggerNotFound {
            trigger_name: trigger_name.to_owned(),
            table_name: table_name.to_owned(),
        })?;

        Ok(schema.triggers.get(trigger_name).cloned().ok_or_else(|| {
            TriggerError::TriggerNotFound {
                trigger_name: trigger_name.to_owned(),
                table_name: table_name.to_owned(),
            }
        })?)
    }
}

#[async_trait(?Send)]
pub trait CustomTriggerMut {
    /// Inserts a trigger into the storage.
    ///
    /// # Arguments
    /// * `trigger` - The trigger to insert.
    async fn insert_trigger(&mut self, _trigger: Trigger) -> Result<()>;

    /// Deletes a trigger from the storage.
    ///
    /// # Arguments
    /// * `trigger_name` - The name of the trigger to delete.
    /// * `table_name` - The name of the table the trigger is associated with.
    async fn delete_trigger(&mut self, _trigger_name: &str, _table_name: &str) -> Result<()>;
}

#[async_trait(?Send)]
impl<G: StoreMut + Store> CustomTriggerMut for G {
    async fn insert_trigger(&mut self, trigger: Trigger) -> Result<()> {
        let table_name = trigger.table_name.clone();
        let schema = self.fetch_schema(&table_name).await?;
        let mut schema = schema.ok_or_else(|| TriggerError::TriggerNotFound {
            trigger_name: trigger.name.clone(),
            table_name: table_name.clone(),
        })?;

        schema.triggers.insert(trigger.name.clone(), trigger);

        self.insert_schema(&schema).await
    }

    async fn delete_trigger(&mut self, trigger_name: &str, table_name: &str) -> Result<()> {
        let schema = self.fetch_schema(table_name).await?;
        let mut schema = schema.ok_or_else(|| TriggerError::TriggerNotFound {
            trigger_name: trigger_name.to_owned(),
            table_name: table_name.to_owned(),
        })?;

        schema.triggers.remove(trigger_name);

        self.insert_schema(&schema).await
    }
}
