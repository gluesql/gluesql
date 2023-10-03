#![cfg(feature = "include-python-workspace")]

use {
    error::GlueSQLError,
    gluesql_core::{
        ast::Statement,
        prelude::{execute, parse, plan, Payload},
        translate::translate,
    },
    payload::{convert, PyPayload},
    pyo3::{prelude::*, types::PyString},
    storages::{
        PyJsonStorage, PyMemoryStorage, PySharedMemoryStorage, PySledStorage, PySledStorageConfig,
        PySledStorageModeConfig, PyStorageEngine,
    },
};

mod error;
mod payload;
mod storages;

#[pyclass(name = "Glue")]
pub struct PyGlue {
    pub storage: PyStorageEngine,
}

macro_rules! plan {
    ($storage:expr, $statement:expr) => {{
        plan(&$storage.0, $statement)
            .await
            .map_err(|e| GlueSQLError::new_err(e.to_string()))
    }};
}

macro_rules! execute {
    ($storage:expr, $statement:expr) => {{
        execute(&mut $storage.0, $statement)
            .await
            .map_err(|e| GlueSQLError::new_err(e.to_string()))
    }};
}

impl PyGlue {
    #[tokio::main]
    pub async fn plan(&self, statement: Statement) -> PyResult<Statement> {
        let storage = &self.storage;

        match storage {
            PyStorageEngine::MemoryStorage(storage) => plan!(storage, statement),
            PyStorageEngine::JsonStorage(storage) => plan!(storage, statement),
            PyStorageEngine::SharedMemoryStorage(storage) => plan!(storage, statement),
            PyStorageEngine::SledStorage(storage) => plan!(storage, statement),
        }
    }

    #[tokio::main]
    pub async fn execute(&mut self, statement: Statement) -> PyResult<Payload> {
        let storage = &mut self.storage;

        match storage {
            PyStorageEngine::MemoryStorage(storage) => execute!(storage, &statement),
            PyStorageEngine::JsonStorage(storage) => execute!(storage, &statement),
            PyStorageEngine::SharedMemoryStorage(storage) => execute!(storage, &statement),
            PyStorageEngine::SledStorage(storage) => execute!(storage, &statement),
        }
    }
}

#[pymethods]
impl PyGlue {
    #[new]
    pub fn new(storage: PyStorageEngine) -> Self {
        PyGlue { storage }
    }

    pub fn query(&mut self, py: Python, sql: &PyString) -> PyResult<PyObject> {
        let sql = sql.to_string();
        let queries = parse(&sql).map_err(|e| GlueSQLError::new_err(e.to_string()))?;

        let mut payloads: Vec<PyPayload> = vec![];
        for query in queries.iter() {
            let statement = translate(query).map_err(|e| GlueSQLError::new_err(e.to_string()))?;
            let statement = self.plan(statement)?;

            let payload = self.execute(statement)?;

            payloads.push(PyPayload { payload });
        }

        Ok(convert(py, payloads))
    }
}

#[pymodule]
fn gluesql(py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<PyGlue>()?;
    m.add_class::<PyMemoryStorage>()?;
    m.add_class::<PyJsonStorage>()?;
    m.add_class::<PySharedMemoryStorage>()?;
    m.add_class::<PySledStorage>()?;
    m.add_class::<PySledStorageConfig>()?;
    m.add_class::<PySledStorageModeConfig>()?;

    m.add("GlueSQLError", py.get_type::<GlueSQLError>())?;
    Ok(())
}
