use crate::storages::ExStorage;
use rustler::NifStruct;

#[derive(NifStruct)]
#[module = "GlueSQL.Native.Glue"]
pub struct ExGlue {
    pub storage: ExStorage,
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn glue_new(storage: ExStorage) -> ExGlue {
    ExGlue { storage }
}
