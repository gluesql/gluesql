use error::{
    EngineNotLoadedError, ExecuteError, GlueSQLError, ParsingError, PlanError, TranslateError,
};
use payload::{convert, PyPayload};

use gluesql_core::{
    ast::Statement,
    prelude::{execute, parse, plan, Payload},
    translate::translate,
};
use pyo3::{prelude::*, types::PyString};
use storages::{
    PyJsonStorage, PyMemoryStorage, PySharedMemoryStorage, PySledStorage, PySledStorageConfig,
    PySledStorageModeConfig, PyStorageEngine,
};
mod error;
mod payload;
mod storages;

#[pyclass(name = "Glue")]
pub struct PyGlue {
    pub storage: Option<PyStorageEngine>,
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
        let storage = self.storage.as_ref().unwrap();

        match storage {
            PyStorageEngine::MemoryStorage(storage) => plan!(storage, statement),
            PyStorageEngine::JsonStorage(storage) => plan!(storage, statement),
            PyStorageEngine::SharedMemoryStorage(storage) => plan!(storage, statement),
            PyStorageEngine::SledStorage(storage) => plan!(storage, statement),
        }
    }

    #[tokio::main]
    pub async fn execute(&mut self, statement: Statement) -> PyResult<Payload> {
        let storage = self.storage.as_mut().unwrap();

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
        if self.storage.is_none() {
            return Err(EngineNotLoadedError::new_err(
                "Storage engine not loaded, please call `set_default_engine` first to load a storage engine.",
            ));
        }

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
fn gluesql(py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<PyGlue>()?;
    m.add_class::<PyMemoryStorage>()?;
    m.add_class::<PyJsonStorage>()?;
    m.add_class::<PySharedMemoryStorage>()?;
    m.add_class::<PySledStorage>()?;
    m.add_class::<PySledStorageConfig>()?;
    m.add_class::<PySledStorageModeConfig>()?;

    m.add("GlueSQLError", py.get_type::<GlueSQLError>())?;
    m.add(
        "EngineNotLoadedError",
        py.get_type::<EngineNotLoadedError>(),
    )?;
    m.add("PlanError", py.get_type::<PlanError>())?;
    m.add("ExecuteError", py.get_type::<ExecuteError>())?;
    m.add("TranslateError", py.get_type::<TranslateError>())?;
    m.add("ParsingError", py.get_type::<ParsingError>())?;
    Ok(())
}
