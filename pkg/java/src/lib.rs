use std::sync::Arc;
use tokio::runtime::Runtime;

use gluesql_core::prelude::{
    Payload as CorePayload, PayloadVariable, execute, parse, plan, translate,
};
use gluesql_json_storage::JsonStorage;
use gluesql_memory_storage::MemoryStorage;
use gluesql_shared_memory_storage::SharedMemoryStorage;
use gluesql_sled_storage::{SledStorage, sled::Config};

#[derive(thiserror::Error, Debug)]
pub enum GlueSQLError {
    #[error("Storage error: {0}")]
    StorageError(String),
    #[error("Execute error: {0}")]
    ExecuteError(String),
    #[error("Translate error: {0}")]
    TranslateError(String),
    #[error("Plan error: {0}")]
    PlanError(String),
    #[error("Parse error: {0}")]
    ParseError(String),
    #[error("Value error: {0}")]
    ValueError(String),
}

pub struct SledConfig {
    pub path: Option<String>,
    pub mode: Option<String>,
}

pub enum Storage {
    Memory,
    Json {
        path: String,
    },
    Sled {
        path: String,
        config: Option<SledConfig>,
    },
    SharedMemory {
        namespace: String,
    },
}

#[derive(serde::Serialize)]
pub enum Payload {
    Create {
        rows: u64,
    },
    Insert {
        rows: u64,
    },
    Update {
        rows: u64,
    },
    Delete {
        rows: u64,
    },
    Select {
        rows: Vec<Vec<String>>,
        labels: Vec<String>,
    },
    DropTable {
        count: u64,
    },
    AlterTable,
    StartTransaction,
    Commit,
    Rollback,
    ShowVariable {
        name: String,
        value: String,
    },
    ShowColumns {
        columns: Vec<String>,
    },
}

fn convert_payload(payload: CorePayload) -> Payload {
    match payload {
        CorePayload::Create => Payload::Create { rows: 0 },
        CorePayload::Insert(rows) => Payload::Insert { rows: rows as u64 },
        CorePayload::Update(rows) => Payload::Update { rows: rows as u64 },
        CorePayload::Delete(rows) => Payload::Delete { rows: rows as u64 },
        CorePayload::Select { labels, rows } => {
            let converted_rows: Vec<Vec<String>> = rows
                .into_iter()
                .map(|row| {
                    row.into_iter()
                        .map(|value| {
                            let debug_str = format!("{:?}", value);
                            // Remove Rust type prefixes like I64(), Str(), etc.
                            if debug_str.starts_with("I64(") && debug_str.ends_with(")") {
                                debug_str[4..debug_str.len() - 1].to_string()
                            } else if debug_str.starts_with("Str(") && debug_str.ends_with(")") {
                                let inner = &debug_str[5..debug_str.len() - 2]; // Remove Str(" and ")
                                inner.to_string()
                            } else if debug_str.starts_with("F64(") && debug_str.ends_with(")") {
                                debug_str[4..debug_str.len() - 1].to_string()
                            } else if debug_str.starts_with("Bool(") && debug_str.ends_with(")") {
                                debug_str[5..debug_str.len() - 1].to_string()
                            } else if debug_str == "Null" {
                                "null".to_string()
                            } else {
                                debug_str
                            }
                        })
                        .collect()
                })
                .collect();
            Payload::Select {
                rows: converted_rows,
                labels,
            }
        }
        CorePayload::DropTable(count) => Payload::DropTable {
            count: count as u64,
        },
        CorePayload::AlterTable => Payload::AlterTable,
        CorePayload::StartTransaction => Payload::StartTransaction,
        CorePayload::Commit => Payload::Commit,
        CorePayload::Rollback => Payload::Rollback,
        CorePayload::ShowVariable(var) => {
            let (name, value) = match var {
                PayloadVariable::Tables(tables) => ("TABLES".to_string(), format!("{:?}", tables)),
                PayloadVariable::Functions(functions) => {
                    ("FUNCTIONS".to_string(), format!("{:?}", functions))
                }
                PayloadVariable::Version(version) => ("VERSION".to_string(), version),
            };
            Payload::ShowVariable { name, value }
        }
        CorePayload::ShowColumns(columns) => {
            let column_names: Vec<String> = columns.into_iter().map(|(name, _)| name).collect();
            Payload::ShowColumns {
                columns: column_names,
            }
        }
        _ => Payload::AlterTable, // Handle other variants
    }
}

pub enum StorageBackend {
    Memory(Arc<tokio::sync::Mutex<MemoryStorage>>),
    Json(Arc<tokio::sync::Mutex<JsonStorage>>),
    SharedMemory(Arc<tokio::sync::Mutex<SharedMemoryStorage>>),
    Sled(Arc<tokio::sync::Mutex<SledStorage>>),
}

pub struct Glue {
    storage: StorageBackend,
    runtime: Arc<Runtime>,
}

impl Glue {
    pub fn new(storage: Storage) -> Result<Self, GlueSQLError> {
        let runtime = Arc::new(Runtime::new().expect("Failed to create async runtime"));

        let storage_backend = match storage {
            Storage::Memory => {
                StorageBackend::Memory(Arc::new(tokio::sync::Mutex::new(MemoryStorage::default())))
            }
            Storage::Json { path } => {
                let json_storage = JsonStorage::new(&path)
                    .map_err(|e| GlueSQLError::StorageError(e.to_string()))?;
                StorageBackend::Json(Arc::new(tokio::sync::Mutex::new(json_storage)))
            }
            Storage::SharedMemory { namespace: _ } => StorageBackend::SharedMemory(Arc::new(
                tokio::sync::Mutex::new(SharedMemoryStorage::new()),
            )),
            Storage::Sled { path, config } => {
                let sled_config = if let Some(config) = config {
                    let mut cfg = Config::default();
                    if let Some(path) = &config.path {
                        cfg = cfg.path(path);
                    }
                    cfg
                } else {
                    Config::default().path(&path)
                };

                let sled_storage = SledStorage::try_from(sled_config)
                    .map_err(|e| GlueSQLError::StorageError(e.to_string()))?;
                StorageBackend::Sled(Arc::new(tokio::sync::Mutex::new(sled_storage)))
            }
        };

        Ok(Self {
            storage: storage_backend,
            runtime,
        })
    }

    pub fn query(&self, sql: String) -> Result<Vec<String>, GlueSQLError> {
        let runtime = Arc::clone(&self.runtime);

        runtime.block_on(async {
            let queries = parse(&sql).map_err(|e| GlueSQLError::ParseError(e.to_string()))?;

            let mut results = Vec::new();

            for query in queries {
                let statement =
                    translate(&query).map_err(|e| GlueSQLError::TranslateError(e.to_string()))?;

                let payload = match &self.storage {
                    StorageBackend::Memory(storage) => {
                        let mut storage_guard = storage.lock().await;
                        let planned_statement = plan(&*storage_guard, statement)
                            .await
                            .map_err(|e| GlueSQLError::PlanError(e.to_string()))?;
                        execute(&mut *storage_guard, &planned_statement)
                            .await
                            .map_err(|e| GlueSQLError::ExecuteError(e.to_string()))?
                    }
                    StorageBackend::Json(storage) => {
                        let mut storage_guard = storage.lock().await;
                        let planned_statement = plan(&*storage_guard, statement)
                            .await
                            .map_err(|e| GlueSQLError::PlanError(e.to_string()))?;
                        execute(&mut *storage_guard, &planned_statement)
                            .await
                            .map_err(|e| GlueSQLError::ExecuteError(e.to_string()))?
                    }
                    StorageBackend::SharedMemory(storage) => {
                        let mut storage_guard = storage.lock().await;
                        let planned_statement = plan(&*storage_guard, statement)
                            .await
                            .map_err(|e| GlueSQLError::PlanError(e.to_string()))?;
                        execute(&mut *storage_guard, &planned_statement)
                            .await
                            .map_err(|e| GlueSQLError::ExecuteError(e.to_string()))?
                    }
                    StorageBackend::Sled(storage) => {
                        let mut storage_guard = storage.lock().await;
                        let planned_statement = plan(&*storage_guard, statement)
                            .await
                            .map_err(|e| GlueSQLError::PlanError(e.to_string()))?;
                        execute(&mut *storage_guard, &planned_statement)
                            .await
                            .map_err(|e| GlueSQLError::ExecuteError(e.to_string()))?
                    }
                };

                let converted = convert_payload(payload);
                results.push(
                    serde_json::to_string(&converted)
                        .map_err(|e| GlueSQLError::ValueError(e.to_string()))?,
                );
            }

            Ok(results)
        })
    }
}

uniffi::include_scaffolding!("gluesql");
