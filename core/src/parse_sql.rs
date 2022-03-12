use {
    crate::result::{Error, Result},
    sqlparser::{
        ast::{Expr as SqlExpr, Query as SqlQuery, Statement as SqlStatement},
        dialect::GenericDialect,
        parser::Parser,
        tokenizer::Tokenizer,
    },
};

pub fn parse(sql: &str) -> Result<Vec<SqlStatement>> {
    let dialect = GenericDialect {};

    Parser::parse_sql(&dialect, sql).map_err(|e| Error::Parser(format!("{:#?}", e)))
}

pub fn parse_query(sql_expr: &str) -> Result<SqlQuery> {
    let dialect = GenericDialect {};
    let tokens = Tokenizer::new(&dialect, sql_expr)
        .tokenize()
        .map_err(|e| Error::Parser(format!("{:#?}", e)))?;

    Parser::new(tokens, &dialect)
        .parse_query()
        .map_err(|e| Error::Parser(format!("{:#?}", e)))
}

pub fn parse_expr(sql_expr: &str) -> Result<SqlExpr> {
    let dialect = GenericDialect {};
    let tokens = Tokenizer::new(&dialect, sql_expr)
        .tokenize()
        .map_err(|e| Error::Parser(format!("{:#?}", e)))?;

    Parser::new(tokens, &dialect)
        .parse_expr()
        .map_err(|e| Error::Parser(format!("{:#?}", e)))
}

pub fn parse_interval(sql_interval: &str) -> Result<SqlExpr> {
    let dialect = GenericDialect {};
    let tokens = Tokenizer::new(&dialect, sql_interval)
        .tokenize()
        .map_err(|e| Error::Parser(format!("{:#?}", e)))?;

    Parser::new(tokens, &dialect)
        .parse_literal_interval()
        .map_err(|e| Error::Parser(format!("{:#?}", e)))
}
