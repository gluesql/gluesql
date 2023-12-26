pub mod memory_storage;

use {
    self::memory_storage::ExMemoryStorage,
    crate::result::ExResult,
    gluesql_core::{
        ast::Statement,
        executor::{execute, Payload},
        plan::plan,
    },
    rustler::NifUntaggedEnum,
    std::ops::{Deref, DerefMut},
};

#[derive(NifUntaggedEnum)]
pub enum ExStorage {
    MemoryStorage(ExMemoryStorage),
}

#[tokio::main]
pub async fn storage_plan(storage: &ExStorage, statement: Statement) -> ExResult<Statement> {
    match storage {
        ExStorage::MemoryStorage(storage) => {
            let lock = storage.resource.locked_storage.read().unwrap();
            let storage = lock.deref();

            plan(storage, statement).await.map_err(|e| e.to_string())
        }
    }
}

#[tokio::main]
pub async fn storage_execute(storage: &mut ExStorage, statement: Statement) -> ExResult<Payload> {
    match storage {
        ExStorage::MemoryStorage(storage) => {
            let mut lock = storage.resource.locked_storage.write().unwrap();
            let mut_storage = lock.deref_mut();

            execute(mut_storage, &statement)
                .await
                .map_err(|e| e.to_string())
        }
    }
}
