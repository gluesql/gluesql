use {
    json_storage::JsonStorage,
    memory_storage::MemoryStorage,
    pyo3::{prelude::*, types::PyString},
    shared_memory_storage::SharedMemoryStorage,
    sled_storage::{sled, SledStorage},
    std::path::PathBuf,
};

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

    pub fn __repr__(&self) -> PyResult<String> {
        Ok(format!("{:?}", self.0))
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

    pub fn __repr__(&self) -> PyResult<String> {
        Ok(format!("{:?}", self.0))
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

    pub fn __repr__(&self) -> PyResult<String> {
        Ok(format!("{:?}", self.0))
    }
}

#[pyclass(name = "SledStorageConfigMode")]
#[derive(Clone, Debug)]
pub struct PySledStorageModeConfig(pub sled::Mode);

#[pymethods]
impl PySledStorageModeConfig {
    pub fn __repr__(&self) -> PyResult<String> {
        match self.0 {
            sled::Mode::LowSpace => Ok("LowSpace".to_string()),
            sled::Mode::HighThroughput => Ok("HighThroughput".to_string()),
        }
    }
}

impl Default for PySledStorageModeConfig {
    fn default() -> Self {
        PySledStorageModeConfig(sled::Mode::LowSpace)
    }
}

#[pyclass(name = "SledStorageConfig")]
#[derive(Clone, Default, Debug)]
pub struct PySledStorageConfig {
    #[pyo3(get, set)]
    pub cache_capacity: u64,

    #[pyo3(get, set)]
    pub path: String,

    #[pyo3(get, set)]
    pub create_new: bool,

    #[pyo3(get, set)]
    pub mode: PySledStorageModeConfig,

    #[pyo3(get, set)]
    pub temporary: bool,

    #[pyo3(get, set)]
    pub use_compression: bool,

    #[pyo3(get, set)]
    pub compression_factor: i32,

    #[pyo3(get, set)]
    pub print_profile_on_drop: bool,
}

#[pymethods]
impl PySledStorageConfig {
    #[new]
    pub fn new() -> Self {
        PySledStorageConfig::default()
    }

    pub fn __repr__(&self) -> PyResult<String> {
        Ok(format!("{:?}", self))
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
        let sled_cfg = sled::Config::default()
            .cache_capacity(cfg.cache_capacity)
            .compression_factor(cfg.compression_factor)
            .create_new(cfg.create_new)
            .mode(cfg.mode.0)
            .path(cfg.path.to_owned())
            .print_profile_on_drop(cfg.print_profile_on_drop)
            .temporary(cfg.temporary)
            .use_compression(cfg.use_compression);

        let storage = SledStorage::try_from(sled_cfg).unwrap();
        Ok(PySledStorage(storage))
    }

    pub fn __repr__(&self) -> PyResult<String> {
        Ok(format!("{:?}", self.0))
    }
}
