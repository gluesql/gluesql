use crate::{
    payload::convert,
    result::{map_glue_result, ExResult},
    storages::{storage_execute, storage_plan, ExStorage},
};
use gluesql_core::prelude::{parse, translate, Payload};
use rustler::{NifStruct, Term};

#[derive(NifStruct)]
#[module = "GlueSQL.Native.Glue"]
pub struct ExGlue {
    pub storage: ExStorage,
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn glue_new(storage: ExStorage) -> ExGlue {
    ExGlue { storage }
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn glue_query<'a>(glue: ExGlue, sql: String) -> ExResult<Term<'a>> {
    let mut storage = glue.storage;

    map_glue_result(parse(sql))?
        .iter()
        .map(|query| {
            map_glue_result(translate(query))
                .and_then(|st| storage_plan(&storage, st))
                .and_then(|st| storage_execute(&mut storage, st))
        })
        .collect::<ExResult<Vec<Payload>>>()
        .and_then(|payloads| Ok(convert(payloads)))
}
