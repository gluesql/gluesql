use {
    crate::result::{Error, Result},
    sqlparser::{
        ast::{
            ColumnDef as SqlColumnDef, DataType as SqlDataType, Expr as SqlExpr, OrderByExpr,
            Query as SqlQuery, SelectItem as SqlSelectItem, Statement as SqlStatement,
        },
        dialect::GenericDialect,
        parser::Parser,
        tokenizer::Tokenizer,
    },
};

const DIALECT: GenericDialect = GenericDialect {};

pub fn parse<Sql: AsRef<str>>(sql: Sql) -> Result<Vec<SqlStatement>> {
    Parser::parse_sql(&DIALECT, sql.as_ref()).map_err(|e| Error::Parser(format!("{:#?}", e)))
}

pub fn parse_query<Sql: AsRef<str>>(sql_expr: Sql) -> Result<SqlQuery> {
    let tokens = Tokenizer::new(&DIALECT, sql_expr.as_ref())
        .tokenize()
        .map_err(|e| Error::Parser(format!("{:#?}", e)))?;

    Parser::new(tokens, &DIALECT)
        .parse_query()
        .map_err(|e| Error::Parser(format!("{:#?}", e)))
}

pub fn parse_expr<Sql: AsRef<str>>(sql_expr: Sql) -> Result<SqlExpr> {
    let tokens = Tokenizer::new(&DIALECT, sql_expr.as_ref())
        .tokenize()
        .map_err(|e| Error::Parser(format!("{:#?}", e)))?;

    Parser::new(tokens, &DIALECT)
        .parse_expr()
        .map_err(|e| Error::Parser(format!("{:#?}", e)))
}

pub fn parse_comma_separated_exprs<Sql: AsRef<str>>(sql_exprs: Sql) -> Result<Vec<SqlExpr>> {
    let tokens = Tokenizer::new(&DIALECT, sql_exprs.as_ref())
        .tokenize()
        .map_err(|e| Error::Parser(format!("{:#?}", e)))?;

    Parser::new(tokens, &DIALECT)
        .parse_comma_separated(Parser::parse_expr)
        .map_err(|e| Error::Parser(format!("{:#?}", e)))
}

pub fn parse_select_item<Sql: AsRef<str>>(sql_select_item: Sql) -> Result<SqlSelectItem> {
    let tokens = Tokenizer::new(&DIALECT, sql_select_item.as_ref())
        .tokenize()
        .map_err(|e| Error::Parser(format!("{:#?}", e)))?;

    Parser::new(tokens, &DIALECT)
        .parse_select_item()
        .map_err(|e| Error::Parser(format!("{:#?}", e)))
}

pub fn parse_select_items<Sql: AsRef<str>>(sql_select_items: Sql) -> Result<Vec<SqlSelectItem>> {
    let tokens = Tokenizer::new(&DIALECT, sql_select_items.as_ref())
        .tokenize()
        .map_err(|e| Error::Parser(format!("{:#?}", e)))?;

    Parser::new(tokens, &DIALECT)
        .parse_comma_separated(Parser::parse_select_item)
        .map_err(|e| Error::Parser(format!("{:#?}", e)))
}

pub fn parse_interval<Sql: AsRef<str>>(sql_interval: Sql) -> Result<SqlExpr> {
    let tokens = Tokenizer::new(&DIALECT, sql_interval.as_ref())
        .tokenize()
        .map_err(|e| Error::Parser(format!("{:#?}", e)))?;

    Parser::new(tokens, &DIALECT)
        .parse_literal_interval()
        .map_err(|e| Error::Parser(format!("{:#?}", e)))
}

pub fn parse_order_by_expr<Sql: AsRef<str>>(sql_order_by_expr: Sql) -> Result<OrderByExpr> {
    let tokens = Tokenizer::new(&DIALECT, sql_order_by_expr.as_ref())
        .tokenize()
        .map_err(|e| Error::Parser(format!("{:#?}", e)))?;

    Parser::new(tokens, &DIALECT)
        .parse_order_by_expr()
        .map_err(|e| Error::Parser(format!("{:#?}", e)))
}

// pub fn parse_column_option_def<Sql: AsRef<str>>(
//     sql_column_option_def: Sql,
// ) -> Result<SqlColumnOptionDef> {
//     let tokens = Tokenizer::new(&DIALECT, sql_column_option_def.as_ref())
//         .tokenize()
//         .map_err(|e| Error::Parser(format!("{:#?}", e)))?;
//
//     Parser::new(tokens, &DIALECT)
//         .parse_column_option_def()
//         .map_err(|e| Error::Parser(format!("{:#?}", e)))
// }

pub fn parse_column_def<Sql: AsRef<str>>(sql_column_def: Sql) -> Result<SqlColumnDef> {
    let tokens = Tokenizer::new(&DIALECT, sql_column_def.as_ref())
        .tokenize()
        .map_err(|e| Error::Parser(format!("{:#?}", e)))?;

    Parser::new(tokens, &DIALECT)
        .parse_column_def()
        .map_err(|e| Error::Parser(format!("{:#?}", e)))
}

pub fn parse_data_type<Sql: AsRef<str>>(sql_data_type: Sql) -> Result<SqlDataType> {
    let tokens = Tokenizer::new(&DIALECT, sql_data_type.as_ref())
        .tokenize()
        .map_err(|e| Error::Parser(format!("{:#?}", e)))?;

    Parser::new(tokens, &DIALECT)
        .parse_data_type()
        .map_err(|e| Error::Parser(format!("{:#?}", e)))
}
