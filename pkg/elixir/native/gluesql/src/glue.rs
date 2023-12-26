use {
    crate::{
        payload::convert,
        result::{parse_sql, translate_sql_statement, ExResult},
        storages::{execute_query, plan_query, ExStorage},
    },
    gluesql_core::prelude::Payload,
    rustler::{NifStruct, Term},
};

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

    parse_sql(sql)?
        .iter()
        .map(|statement| {
            translate_sql_statement(statement)
                .and_then(|st| plan_query(&storage, st))
                .and_then(|st| execute_query(&mut storage, st))
        })
        .collect::<ExResult<Vec<Payload>>>()
        .and_then(|payloads| Ok(convert(payloads)))
}
