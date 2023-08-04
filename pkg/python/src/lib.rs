use error::{ExecuteError, ParsingError, PlanError, TranslateError};
use payload::convert;

use gluesql_core::{
    ast::Statement,
    prelude::{execute, parse, plan, Payload},
    translate::translate,
};
use memory_storage::MemoryStorage;
use pyo3::{prelude::*, types::PyString};
mod error;
mod payload;

#[pyclass]
pub struct Glue {
    pub storage: MemoryStorage,
}

#[pyclass]
pub struct PyPayload {
    pub payload: Payload,
}

impl Glue {
    #[tokio::main]
    pub async fn plan(&self, statement: Statement) -> Result<Statement, PyErr> {
        plan(&self.storage, statement)
            .await
            .map_err(|e| PlanError::new_err(e.to_string()))
    }

    #[tokio::main]
    pub async fn execute(&mut self, statement: Statement) -> Result<Payload, PyErr> {
        execute(&mut self.storage, &statement)
            .await
            .map_err(|e| ExecuteError::new_err(e.to_string()))
    }
}

#[pymethods]
impl Glue {
    #[new]
    pub fn new() -> Self {
        let storage = MemoryStorage::default();
        Glue { storage }
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
    m.add_class::<Glue>()?;
    Ok(())
}
