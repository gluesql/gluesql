use gluesql_core::{
    ast::Statement, parse_sql::parse, prelude::Result as GlueResult,
    sqlparser::ast::Statement as SqlStatement, translate::translate,
};

pub type ExResult<T> = std::result::Result<T, String>;

/// Map gluesql result type to custom result type.
pub fn map_result<T>(result: GlueResult<T>) -> ExResult<T> {
    result.map_err(|e| e.to_string())
}

/// GlueSQL's `parse()` function, remapped with `ExResult` for better abstraction
pub fn parse_sql<Sql>(sql: Sql) -> ExResult<Vec<SqlStatement>>
where
    Sql: AsRef<str>,
{
    map_result(parse(sql))
}

/// GlueSQL's `translate()` function, remapped with `ExResult` for better abstraction
pub fn translate_sql_statement(statement: &SqlStatement) -> ExResult<Statement> {
    map_result(translate(statement))
}
