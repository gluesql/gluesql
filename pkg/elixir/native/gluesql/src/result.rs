use {gluesql_core::prelude::Result as GlueResult, std::result::Result};

pub type ExResult<T> = Result<T, String>;

pub fn map_glue_result<T>(result: GlueResult<T>) -> ExResult<T> {
    result.map_err(|e| e.to_string())
}
