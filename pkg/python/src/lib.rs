use payload::convert;

use gluesql_core::{
    ast::Statement,
    prelude::{execute, parse, plan, Payload},
    translate::translate,
};
use memory_storage::MemoryStorage;
use pyo3::{prelude::*, types::PyString};
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
    pub async fn plan(&self, statement: Statement) -> Statement {
        plan(&self.storage, statement).await.unwrap()
    }

    #[tokio::main]
    pub async fn execute(&mut self, statement: Statement) -> Payload {
        execute(&mut self.storage, &statement).await.unwrap()
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
        let queries = parse(&sql).unwrap();

        let mut payloads: Vec<PyPayload> = vec![];
        for query in &queries {
            let statement = translate(query).unwrap();
            let statement = self.plan(statement);

            let payload = self.execute(statement);

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
