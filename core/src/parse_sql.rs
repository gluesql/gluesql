use {
    crate::result::{Error, Result},
    sqlparser::{
        ast::{Expr as SqlExpr, Query as SqlQuery, Statement as SqlStatement},
        dialect::GenericDialect,
        parser::Parser,
        tokenizer::Tokenizer,
    },
};

pub fn parse<Sql: AsRef<str>>(sql: Sql) -> Result<Vec<SqlStatement>> {
    let dialect = GenericDialect {};

    Parser::parse_sql(&dialect, sql.as_ref()).map_err(|e| Error::Parser(format!("{:#?}", e)))
}

pub fn parse_query<Sql: AsRef<str>>(sql_expr: Sql) -> Result<SqlQuery> {
    let dialect = GenericDialect {};
    let tokens = Tokenizer::new(&dialect, sql_expr.as_ref())
        .tokenize()
        .map_err(|e| Error::Parser(format!("{:#?}", e)))?;

    Parser::new(tokens, &dialect)
        .parse_query()
        .map_err(|e| Error::Parser(format!("{:#?}", e)))
}

pub fn parse_expr<Sql: AsRef<str>>(sql_expr: Sql) -> Result<SqlExpr> {
    let dialect = GenericDialect {};
    let tokens = Tokenizer::new(&dialect, sql_expr.as_ref())
        .tokenize()
        .map_err(|e| Error::Parser(format!("{:#?}", e)))?;

    Parser::new(tokens, &dialect)
        .parse_expr()
        .map_err(|e| Error::Parser(format!("{:#?}", e)))
}

pub fn parse_interval<Sql: AsRef<str>>(sql_interval: Sql) -> Result<SqlExpr> {
    let dialect = GenericDialect {};
    let tokens = Tokenizer::new(&dialect, sql_interval.as_ref())
        .tokenize()
        .map_err(|e| Error::Parser(format!("{:#?}", e)))?;

    Parser::new(tokens, &dialect)
        .parse_literal_interval()
        .map_err(|e| Error::Parser(format!("{:#?}", e)))
}
