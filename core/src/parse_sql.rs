use {
    crate::result::{Error, Result},
    sqlparser::{
        ast::{
            Assignment as SqlAssignment, ColumnDef as SqlColumnDef, DataType as SqlDataType,
            Expr as SqlExpr, Ident as SqlIdent, OrderByExpr as SqlOrderByExpr, Query as SqlQuery,
            SelectItem as SqlSelectItem, Statement as SqlStatement,
        },
        dialect::PostgreSqlDialect,
        parser::Parser,
        tokenizer::Tokenizer,
    },
};

const DIALECT: PostgreSqlDialect = PostgreSqlDialect {};

pub fn parse<Sql: AsRef<str>>(sql: Sql) -> Result<Vec<SqlStatement>> {
    Parser::parse_sql(&DIALECT, sql.as_ref()).map_err(|e| Error::Parser(format!("{:#?}", e)))
}

macro_rules! generate_parse_fn {
    ($fn_name: ident, $output_type: ty) => {
        pub fn $fn_name<Sql: AsRef<str>>(sql_expr: Sql) -> Result<$output_type> {
            let tokens = Tokenizer::new(&DIALECT, sql_expr.as_ref())
                .tokenize()
                .map_err(|e| Error::Parser(format!("{:#?}", e)))?;

            Parser::new(&DIALECT)
                .with_tokens(tokens)
                .$fn_name()
                .map_err(|e| Error::Parser(format!("{:#?}", e)))
        }
    };
    ($fn_name: ident, $parse_fn_name: ident, $parse_fn_arg: ident, $output_type: ty) => {
        pub fn $fn_name<Sql: AsRef<str>>(sql_expr: Sql) -> Result<$output_type> {
            let tokens = Tokenizer::new(&DIALECT, sql_expr.as_ref())
                .tokenize()
                .map_err(|e| Error::Parser(format!("{:#?}", e)))?;

            Parser::new(&DIALECT)
                .with_tokens(tokens)
                .$parse_fn_name(Parser::$parse_fn_arg)
                .map_err(|e| Error::Parser(format!("{:#?}", e)))
        }
    };
}

generate_parse_fn!(parse_query, SqlQuery);
generate_parse_fn!(parse_expr, SqlExpr);
generate_parse_fn!(
    parse_comma_separated_exprs,
    parse_comma_separated,
    parse_expr,
    Vec<SqlExpr>
);
generate_parse_fn!(parse_select_item, SqlSelectItem);
generate_parse_fn!(
    parse_select_items,
    parse_comma_separated,
    parse_select_item,
    Vec<SqlSelectItem>
);
generate_parse_fn!(parse_interval, SqlExpr);
generate_parse_fn!(parse_order_by_expr, SqlOrderByExpr);
generate_parse_fn!(
    parse_order_by_exprs,
    parse_comma_separated,
    parse_order_by_expr,
    Vec<SqlOrderByExpr>
);
generate_parse_fn!(parse_column_def, SqlColumnDef);
generate_parse_fn!(parse_data_type, SqlDataType);
generate_parse_fn!(parse_assignment, SqlAssignment);
generate_parse_fn!(parse_identifiers, Vec<SqlIdent>);
