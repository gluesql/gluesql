use {serde::Serialize, std::fmt::Debug, thiserror::Error};

#[derive(Error, Serialize, Debug, PartialEq, Eq)]
pub enum InsertError {
    #[error("table not found: {0}")]
    TableNotFound(String),

    #[error("lack of required column: {0}")]
    LackOfRequiredColumn(String),

    #[error("wrong column name: {0}")]
    WrongColumnName(String),

    #[error("column and values not matched")]
    ColumnAndValuesNotMatched,

    #[error("literals have more values than target columns")]
    TooManyValues,

    #[error("only single value accepted for schemaless row insert: got {0}")]
    OnlySingleValueAcceptedForSchemalessRow(usize),

    #[error("map type required: {0}")]
    MapTypeValueRequired(String),

    #[error(
        "cannot find referenced value on {table_name}.{column_name} with value {referenced_value:?}"
    )]
    CannotFindReferencedValue {
        table_name: String,
        column_name: String,
        referenced_value: String,
    },

    #[error("unreachable referencing column name: {0}")]
    ConflictReferencingColumnName(String),
}
