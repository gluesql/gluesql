use gluesql_core::{
    ast::Statement,
    prelude::{Payload, execute, parse, translate},
    store::Planner,
};

use crate::{error::GlueSQLError, storage::StorageBackend, uniffi_types::QueryResult};

pub struct QueryExecutor;

impl QueryExecutor {
    pub async fn execute_query(
        storage: &StorageBackend,
        sql: String,
    ) -> Result<Vec<QueryResult>, GlueSQLError> {
        let queries = parse(&sql).map_err(|e| GlueSQLError::ParseError(e.to_string()))?;

        let mut results = Vec::new();

        for query in queries {
            let statement =
                translate(&query).map_err(|e| GlueSQLError::TranslateError(e.to_string()))?;

            let payload = Self::execute_statement(storage, statement).await?;
            let result = QueryResult::from(payload);
            results.push(result);
        }

        Ok(results)
    }

    async fn execute_statement(
        storage: &StorageBackend,
        statement: Statement,
    ) -> Result<Payload, GlueSQLError> {
        macro_rules! execute_on_storage {
            ($storage:expr) => {{
                let mut storage_guard = $storage.lock().await;
                let planned_statement = storage_guard
                    .plan(statement)
                    .await
                    .map_err(|e| GlueSQLError::PlanError(e.to_string()))?;
                execute(&mut *storage_guard, &planned_statement)
                    .await
                    .map_err(|e| GlueSQLError::ExecuteError(e.to_string()))
            }};
        }

        match storage {
            StorageBackend::Memory(storage) => execute_on_storage!(storage),
            StorageBackend::Json(storage) => execute_on_storage!(storage),
            StorageBackend::SharedMemory(storage) => execute_on_storage!(storage),
            StorageBackend::Sled(storage) => execute_on_storage!(storage),
        }
    }
}
