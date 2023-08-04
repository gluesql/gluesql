use error::{EngineNotLoadedError, ExecuteError, ParsingError, PlanError, TranslateError};
use payload::convert;

use gluesql_core::{
    ast::Statement,
    prelude::{execute, parse, plan, Payload},
    translate::translate,
};
use pyo3::{prelude::*, types::PyString};
use storages::{PyJsonStorage, PyMemoryStorage, PySharedMemoryStorage, PySledStorage};
mod error;
mod payload;
mod storages;

#[derive(FromPyObject)]
pub enum PyStorageEngine {
    MemoryStorage(PyMemoryStorage),
    JsonStorage(PyJsonStorage),
    SharedMemoryStorage(PySharedMemoryStorage),
    SledStorage(PySledStorage),
}

#[pyclass(name = "Glue")]
pub struct PyGlue {
    pub storage: Option<PyStorageEngine>,
}

#[pyclass]
pub struct PyPayload {
    pub payload: Payload,
}

macro_rules! plan {
    ($storage:expr, $statement:expr) => {{
        plan(&$storage.0, $statement)
            .await
            .map_err(|e| PlanError::new_err(e.to_string()))
    }};
}

macro_rules! execute {
    ($storage:expr, $statement:expr) => {{
        execute(&mut $storage.0, $statement)
            .await
            .map_err(|e| ExecuteError::new_err(e.to_string()))
    }};
}

impl PyGlue {
    #[tokio::main]
    pub async fn plan(&self, statement: Statement) -> PyResult<Statement> {
        let storage = self.storage.as_ref().ok_or(EngineNotLoadedError::new_err(
            "Storage engine not loaded, please call `set_default_engine` first",
        ))?;

        match storage {
            PyStorageEngine::MemoryStorage(storage) => plan!(storage, statement),
            PyStorageEngine::JsonStorage(storage) => plan!(storage, statement),
            PyStorageEngine::SharedMemoryStorage(storage) => plan!(storage, statement),
            PyStorageEngine::SledStorage(storage) => plan!(storage, statement),
        }
    }

    #[tokio::main]
    pub async fn execute(&mut self, statement: Statement) -> PyResult<Payload> {
        let storage = self.storage.as_mut().ok_or(EngineNotLoadedError::new_err(
            "Storage engine not loaded, please call `set_default_engine` first",
        ))?;

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
    pub fn new() -> Self {
        PyGlue { storage: None }
    }

    pub fn set_default_engine(&mut self, default_engine: PyStorageEngine) {
        self.storage = Some(default_engine);
    }

    pub fn query(&mut self, py: Python, sql: &PyString) -> PyResult<PyObject> {
        let sql = sql.to_string();
        let queries = parse(&sql).map_err(|e| ParsingError::new_err(e.to_string()))?;

        let mut payloads: Vec<PyPayload> = vec![];
        for query in queries.iter() {
            let statement = translate(query).map_err(|e| TranslateError::new_err(e.to_string()))?;
            let statement = self.plan(statement)?;

            let payload = self.execute(statement)?;

            payloads.push(PyPayload { payload });
        }

        Ok(convert(py, payloads))
    }
}

#[pymodule]
fn gluesql(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<PyGlue>()?;
    m.add_class::<PyMemoryStorage>()?;
    m.add_class::<PyJsonStorage>()?;
    m.add_class::<PySharedMemoryStorage>()?;
    m.add_class::<PySledStorage>()?;
    Ok(())
}
