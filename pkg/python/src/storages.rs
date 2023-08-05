use pyo3::{prelude::*, types::PyString};
use std::path::PathBuf;

use json_storage::JsonStorage;
use memory_storage::MemoryStorage;
use shared_memory_storage::SharedMemoryStorage;
use sled_storage::{SledStorage, sled};

#[derive(FromPyObject)]
pub enum PyStorageEngine {
    MemoryStorage(PyMemoryStorage),
    JsonStorage(PyJsonStorage),
    SharedMemoryStorage(PySharedMemoryStorage),
    SledStorage(PySledStorage),
}

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

#[pyclass(name = "SledStorageConfigMode")]
#[derive(Clone)]
pub struct PySledStorageModeConfig(pub sled::Mode);

#[pymethods]
impl PySledStorageModeConfig {
    // TODO: Implement this enum.
}

#[pyclass(name = "SledStorageConfig")]
#[derive(Clone)]
pub struct PySledStorageConfig(pub sled::Config);

#[pymethods]
impl PySledStorageConfig {
    #[new]
    pub fn new() -> PyResult<Self> {
        Ok(PySledStorageConfig(sled::Config::default()))
    }

    pub fn path(&mut self, py: Python, path_arg: &PyString) -> PyResult<PyObject> {
        let path_str = path_arg.to_str()?;
        Ok(PySledStorageConfig(self.0.clone().path(path_str)).into_py(py))
    }

    pub fn temporary(&mut self, py: Python, v: bool) -> PyResult<PyObject> {
        Ok(PySledStorageConfig(self.0.clone().temporary(v)).into_py(py))
    }

    pub fn use_compression(&mut self, py: Python, v: bool) -> PyResult<PyObject> {
        Ok(PySledStorageConfig(self.0.clone().use_compression(v)).into_py(py))
    }

    pub fn print_profile_on_drop(&mut self, py: Python, v: bool) -> PyResult<PyObject> {
        Ok(PySledStorageConfig(self.0.clone().print_profile_on_drop(v)).into_py(py))
    }

    pub fn compression_factor(&mut self, py: Python, v: i32) -> PyResult<PyObject> {
        Ok(PySledStorageConfig(self.0.clone().compression_factor(v)).into_py(py))
    }

    pub fn create_new(&mut self, py: Python, v: bool) -> PyResult<PyObject> {
        Ok(PySledStorageConfig(self.0.clone().create_new(v)).into_py(py))
    }

    pub fn cache_capacity(&mut self, py: Python, v: u64) -> PyResult<PyObject> {
        Ok(PySledStorageConfig(self.0.clone().cache_capacity(v)).into_py(py))
    }

    pub fn mode(&mut self, py: Python, mode: &PySledStorageModeConfig) -> PyResult<PyObject> {
        Ok(PySledStorageConfig(self.0.clone().mode(mode.0)).into_py(py))
    }

    pub fn flush_every_ms(&mut self, py: Python, every_ms: Option<u64>) -> PyResult<PyObject> {
        Ok(PySledStorageConfig(self.0.clone().flush_every_ms(every_ms)).into_py(py))
    }
}

#[pyclass(name = "SledStorage")]
#[derive(Clone)]
pub struct PySledStorage(pub SledStorage);

#[pymethods]
impl PySledStorage {
    #[new]
    pub fn new(path_arg: &PyString) -> PyResult<Self> {
        let path_str = path_arg.to_str()?;
        let storage = SledStorage::new(path_str).unwrap();
        Ok(PySledStorage(storage))
    }

    #[staticmethod]
    pub fn try_from(cfg: &PySledStorageConfig) -> PyResult<Self> {
        let storage = SledStorage::try_from(cfg.0.clone()).unwrap();
        Ok(PySledStorage(storage))
    }
}
