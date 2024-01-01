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
};

#[derive(NifUntaggedEnum)]
pub enum ExStorage {
    MemoryStorage(ExMemoryStorage),
}

#[tokio::main]
pub async fn plan_query(storage: &ExStorage, statement: Statement) -> ExResult<Statement> {
    match storage {
        ExStorage::MemoryStorage(storage) => {
            let storage = storage.resource.locked_storage.read().await;

            plan(&*storage, statement).await.map_err(|e| e.to_string())
        }
    }
}

#[tokio::main]
pub async fn execute_query(storage: &mut ExStorage, statement: Statement) -> ExResult<Payload> {
    match storage {
        ExStorage::MemoryStorage(storage) => {
            let mut storage = storage.resource.locked_storage.write().await;

            execute(&mut *storage, &statement)
                .await
                .map_err(|e| e.to_string())
        }
    }
}
