use sqlparser::ast::Statement;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
pub use sqlparser::parser::ParserError;

pub struct Query(pub Statement);

pub fn parse(sql: &str) -> Result<Vec<Query>, ParserError> {
    let dialect = GenericDialect {};

    Parser::parse_sql(&dialect, sql).map(|parsed| parsed.into_iter().map(Query).collect())
}
