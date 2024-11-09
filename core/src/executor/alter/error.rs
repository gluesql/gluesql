use {
    super::table::Referencing,
    crate::ast::{DataType, Expr},
    serde::Serialize,
    std::fmt::Debug,
    thiserror::Error,
};

#[derive(Error, Serialize, Debug, PartialEq, Eq)]
pub enum AlterError {
    // CREATE TABLE
    #[error("table already exists: {0}")]
    TableAlreadyExists(String),

    #[error("function already exists: {0}")]
    FunctionAlreadyExists(String),

    #[error("function does not exist: {0}")]
    FunctionNotFound(String),

    // CREATE INDEX, DROP TABLE
    #[error("table does not exist: {0}")]
    TableNotFound(String),

    #[error("CTAS source table does not exist: {0}")]
    CtasSourceTableNotFound(String),

    // validate column def
    #[error("column '{0}' of data type '{1:?}' is unsupported for unique constraint")]
    UnsupportedDataTypeForUniqueColumn(String, DataType),

    // validate index expr
    #[error("unsupported index expr: {0:#?}")]
    UnsupportedIndexExpr(Expr),

    // validate index expr
    #[error("unsupported unnamed argument")]
    UnsupportedUnnamedArg,

    #[error("identifier not found: {0:#?}")]
    IdentifierNotFound(Expr),

    #[error("duplicate column name: {0}")]
    DuplicateColumnName(String),

    #[error("duplicate arg name: {0}")]
    DuplicateArgName(String),

    #[error("non-default argument should not follow the default argument")]
    NonDefaultArgumentFollowsDefaultArgument,

    #[error("foreign table not found: {0}")]
    ReferencedTableNotFound(String),

    #[error("referenced column not found: {0}")]
    ReferencedColumnNotFound(String),

    #[error("referencing column not found: {0}")]
    ReferencingColumnNotFound(String),

    #[error("referencing column '{referencing_column}' of data type '{referencing_column_type}' does not match referenced column '{referenced_column}' of data type '{referenced_column_type}'")]
    ForeignKeyDataTypeMismatch {
        referencing_column: String,
        referencing_column_type: DataType,
        referenced_column: String,
        referenced_column_type: DataType,
    },

    #[error("referenced column '{referenced_table}.{referenced_column}' is not unique, cannot be used as foreign key")]
    ReferencingNonPKColumn {
        referenced_table: String,
        referenced_column: String,
    },

    #[error("cannot drop table '{referenced_table_name}' due to referencing tables: '{}'", referencings.iter().map(ToString::to_string).collect::<Vec<_>>().join(", "))]
    CannotDropTableWithReferencing {
        referenced_table_name: String,
        referencings: Vec<Referencing>,
    },

    #[error("cannot drop column '{}.{}' referenced by '{}'", referencing.foreign_key.referenced_table_name, referencing.foreign_key.referenced_column_name, referencing)]
    CannotAlterReferencedColumn { referencing: Referencing },

    #[error("cannot drop column '{}.{}' referencing with '{}'", referencing.table_name, referencing.foreign_key.referencing_column_name, referencing)]
    CannotAlterReferencingColumn { referencing: Referencing },
}
