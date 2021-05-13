use {
    crate::result::{Error, Result},
    sqlparser::{ast::Statement as SqlStatement, dialect::GenericDialect, parser::Parser},
};

pub fn parse(sql: &str) -> Result<Vec<SqlStatement>> {
    let dialect = GenericDialect {};

    Parser::parse_sql(&dialect, sql).map_err(|e| Error::Parser(format!("{:#?}", e)))
}
