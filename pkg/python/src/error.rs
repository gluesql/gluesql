use gluesql_core::prelude::Error;
use pyo3::{create_exception, exceptions::PyException, prelude::*, pyclass::CompareOp};

#[pyclass(name = "GlueSQLError")]
pub struct PyGlueSQLError(pub Error);

create_exception!(gluesql, EngineNotLoadedError, PyException);

create_exception!(gluesql, ParsingError, PyException);
create_exception!(gluesql, TranslateError, PyException);
create_exception!(gluesql, ExecuteError, PyException);
create_exception!(gluesql, PlanError, PyException);

#[pymethods]
impl PyGlueSQLError {
    pub fn __richcmp__(&self, py: Python, rhs: &PyGlueSQLError, op: CompareOp) -> PyObject {
        match op {
            CompareOp::Eq => (self.0 == rhs.0).into_py(py),
            CompareOp::Ne => (self.0 != rhs.0).into_py(py),
            _ => py.NotImplemented(),
        }
    }

    pub fn __str__(&self) -> String {
        format!("{}", self.0)
    }
}

impl From<PyGlueSQLError> for PyErr {
    fn from(e: PyGlueSQLError) -> PyErr {
        match e.0 {
            Error::Parser(e) => ParsingError::new_err(e),
            Error::Translate(e) => TranslateError::new_err(e.to_string()),
            Error::Execute(e) => ExecuteError::new_err(e.to_string()),
            Error::Plan(e) => PlanError::new_err(e.to_string()),
            _ => panic!("Unknown error occurred!"),
        }
    }
}
