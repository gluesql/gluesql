use {
    gluesql_core::prelude::Glue,
    memory_storage::MemoryStorage,
    rustler::{NifStruct, ResourceArc},
    std::ops::Deref,
};

#[derive(NifStruct)]
#[module = "GlueSQL.Native.MemoryStorage"]
pub struct ExMemoryStorage {
    pub resource: ResourceArc<ExMemoryStorageRef>,
}

impl ExMemoryStorage {
    pub fn new() -> Self {
        Self {
            resource: ResourceArc::new(ExMemoryStorageRef::new()),
        }
    }
}

// Implement Deref so we can call `Glue<MemoryStorage` functions directly from a `ExMemoryStorage` struct.
impl Deref for ExMemoryStorage {
    type Target = Glue<MemoryStorage>;

    fn deref(&self) -> &Self::Target {
        &self.resource.0
    }
}

pub struct ExMemoryStorageRef(pub Glue<MemoryStorage>);

impl ExMemoryStorageRef {
    fn new() -> Self {
        Self(Glue::new(MemoryStorage::default()))
    }
}
