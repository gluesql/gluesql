//! Triggers to allow the creation/dropping of triggers.

use crate::{
    ast::{CreateTrigger, DropTrigger},
    error::Result,
    store::{GStore, GStoreMut},
};

use super::{
    validate::{validate_arg_names, validate_default_args},
    AlterError,
};

/// Create a trigger in the provided storage.
pub async fn insert_trigger<T: GStore + GStoreMut>(
    storage: &mut T,
    create_trigger: &CreateTrigger,
) -> Result<()> {
    validate_arg_names(&create_trigger.arguments)?;
    validate_default_args(&create_trigger.arguments).await?;

    match storage
        .fetch_trigger(&create_trigger.name, &create_trigger.table_name)
        .await
    {
        Ok(_) => {
            if create_trigger.or_replace {
                storage
                    .delete_trigger(&create_trigger.name, &create_trigger.table_name)
                    .await?;
                storage
                    .insert_trigger(create_trigger.clone().into())
                    .await?;
                Ok(())
            } else {
                Err(AlterError::TriggerAlreadyExists(create_trigger.name.to_owned()).into())
            }
        }
        Err(err) => Err(err),
    }
}

/// Drop a trigger in the provided storage.
pub async fn delete_trigger<T: GStore + GStoreMut>(
    storage: &mut T,
    drop_trigger: &DropTrigger,
) -> Result<()> {
    if !drop_trigger.if_exists {
        let _ = storage
            .fetch_trigger(&drop_trigger.name, &drop_trigger.table_name)
            .await?;
    }

    storage.delete_trigger(&drop_trigger.name, &drop_trigger.table_name).await?;
    Ok(())
}
