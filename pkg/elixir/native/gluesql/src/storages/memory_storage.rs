use {
    memory_storage::MemoryStorage,
    rustler::{NifStruct, ResourceArc},
    std::ops::Deref,
};

#[derive(NifStruct)]
#[module = "GlueSQL.Native.MemoryStorage"]
pub struct ExMemoryStorage {
    pub resource: ResourceArc<ExMemoryStorageRef>,
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn memory_storage_new() -> ExMemoryStorage {
    ExMemoryStorage {
        resource: ResourceArc::new(ExMemoryStorageRef::new()),
    }
}

// Implement Deref so we can call `Glue<MemoryStorage>` functions directly from a `ExMemoryStorage` struct.
impl Deref for ExMemoryStorage {
    type Target = MemoryStorage;

    fn deref(&self) -> &Self::Target {
        &self.resource.0
    }
}

pub struct ExMemoryStorageRef(pub MemoryStorage);

impl ExMemoryStorageRef {
    fn new() -> Self {
        Self(MemoryStorage::default())
    }
}
