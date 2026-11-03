use {super::decode::DecodeError, std::path::PathBuf, thiserror::Error};

/// Errors that describe migration requirements and logical format conversion.
#[derive(Debug, Error)]
pub enum MigrationError {
    #[error(
        "[RedbStorage] migration required (found v{found}, expected v{expected}); migrate redb-storage data to the latest format before opening"
    )]
    MigrationRequired { found: u32, expected: u32 },

    #[error("[RedbStorage] unsupported format version v{0}")]
    UnsupportedFormatVersion(u32),

    #[error("[RedbStorage] unsupported newer format version v{0}")]
    UnsupportedNewerFormatVersion(u32),

    #[error("[RedbStorage] invalid storage format metadata: missing format version key")]
    MissingFormatVersionMetadata,

    #[error("[RedbStorage] failed to parse v1 row payload in table '{0}'")]
    InvalidV1RowPayload(String),

    #[error(
        "[RedbStorage] failed to decode schema in table '{table_name}': {reason}; migration stopped"
    )]
    InvalidSchemaPayload { table_name: String, reason: String },

    #[error(
        "[RedbStorage] ambiguous schema in table '{0}': historical formats produce different schemas; migration stopped without changing schemas or rows"
    )]
    AmbiguousSchemaPayload(String),

    #[error("[RedbStorage] storage path '{}' does not exist", .0.display())]
    StoragePathDoesNotExist(PathBuf),

    #[error("[RedbStorage] storage path '{}' is not a file", .0.display())]
    StoragePathNotFile(PathBuf),
}

// Version-specific migration paths decide how to handle these decode failures.
#[derive(Debug, Error)]
pub(super) enum SchemaDecodeError {
    #[error(transparent)]
    Payload(#[from] DecodeError),

    #[error("table name does not match the schema key")]
    TableNameMismatch,

    #[error("{0}")]
    RecoveryRequired(&'static str),
}
