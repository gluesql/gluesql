use gluesql_core::ast::Statement;
use gluesql_core::prelude::{execute, parse, plan, translate};

use crate::error::GlueSQLError;
use crate::storage::StorageBackend;
use crate::uniffi_types::QueryResult;

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
    ) -> Result<gluesql_core::prelude::Payload, GlueSQLError> {
        match storage {
            StorageBackend::Memory(storage) => {
                let mut storage_guard = storage.lock().await;
                let planned_statement = plan(&*storage_guard, statement)
                    .await
                    .map_err(|e| GlueSQLError::PlanError(e.to_string()))?;
                execute(&mut *storage_guard, &planned_statement)
                    .await
                    .map_err(|e| GlueSQLError::ExecuteError(e.to_string()))
            }
            StorageBackend::Json(storage) => {
                let mut storage_guard = storage.lock().await;
                let planned_statement = plan(&*storage_guard, statement)
                    .await
                    .map_err(|e| GlueSQLError::PlanError(e.to_string()))?;
                execute(&mut *storage_guard, &planned_statement)
                    .await
                    .map_err(|e| GlueSQLError::ExecuteError(e.to_string()))
            }
            StorageBackend::SharedMemory(storage) => {
                let mut storage_guard = storage.lock().await;
                let planned_statement = plan(&*storage_guard, statement)
                    .await
                    .map_err(|e| GlueSQLError::PlanError(e.to_string()))?;
                execute(&mut *storage_guard, &planned_statement)
                    .await
                    .map_err(|e| GlueSQLError::ExecuteError(e.to_string()))
            }
            StorageBackend::Sled(storage) => {
                let mut storage_guard = storage.lock().await;
                let planned_statement = plan(&*storage_guard, statement)
                    .await
                    .map_err(|e| GlueSQLError::PlanError(e.to_string()))?;
                execute(&mut *storage_guard, &planned_statement)
                    .await
                    .map_err(|e| GlueSQLError::ExecuteError(e.to_string()))
            }
        }
    }
}
