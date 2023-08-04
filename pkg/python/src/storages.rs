use std::path::PathBuf;

use json_storage::JsonStorage;
use memory_storage::MemoryStorage;
use pyo3::{prelude::*, types::PyString};
use shared_memory_storage::SharedMemoryStorage;

#[pyclass(name = "MemoryStorage")]
#[derive(Clone)]
pub struct PyMemoryStorage(pub MemoryStorage);

#[pymethods]
impl PyMemoryStorage {
    #[new]
    pub fn new() -> Self {
        PyMemoryStorage(MemoryStorage::default())
    }
}

#[pyclass(name = "JsonStorage")]
#[derive(Clone)]
pub struct PyJsonStorage(pub JsonStorage);

#[pymethods]
impl PyJsonStorage {
    #[new]
    pub fn new(path_arg: &PyString) -> Self {
        let mut path = PathBuf::new();
        path.push(path_arg.to_string());
        PyJsonStorage(JsonStorage { path })
    }
}

#[pyclass(name = "SharedMemoryStorage")]
#[derive(Clone)]
pub struct PySharedMemoryStorage(pub SharedMemoryStorage);

#[pymethods]
impl PySharedMemoryStorage {
    #[new]
    pub fn new() -> Self {
        PySharedMemoryStorage(SharedMemoryStorage::default())
    }
}
