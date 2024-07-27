//! Submodule providing the Trigger-related traits and its implementations.
use {
    crate::{
        data::Trigger,
        result::{Error, Result},
    },
    async_trait::async_trait,
};

#[async_trait(?Send)]
pub trait CustomTrigger {
    async fn fetch_trigger(&self, _trigger_name: &str) -> Result<Option<&Trigger>> {
        Err(Error::StorageMsg(
            "[Storage] CustomTrigger is not supported".to_owned(),
        ))
    }
    async fn fetch_all_triggers(&self) -> Result<Vec<&Trigger>> {
        Err(Error::StorageMsg(
            "[Storage] CustomTrigger is not supported".to_owned(),
        ))
    }
}

#[async_trait(?Send)]
pub trait CustomTriggerMut {
    async fn insert_trigger(&mut self, _trigger: Trigger) -> Result<()> {
        Err(Error::StorageMsg(
            "[Storage] CustomTrigger is not supported".to_owned(),
        ))
    }

    async fn delete_trigger(&mut self, _trigger_name: &str) -> Result<()> {
        Err(Error::StorageMsg(
            "[Storage] CustomTrigger is not supported".to_owned(),
        ))
    }
}
