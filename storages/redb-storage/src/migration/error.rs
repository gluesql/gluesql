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
        "[RedbStorage] cannot migrate to v5: table name '{0}' contains '/'; rename it with a v4-compatible version before retrying"
    )]
    V5TableNameContainsSlash(String),

    #[error(
        "[RedbStorage] failed to decode schema in table '{table_name}': {source}; migration stopped"
    )]
    InvalidSchemaPayload {
        table_name: String,
        source: SchemaDecodeError,
    },

    #[error(
        "[RedbStorage] failed to decode schema in table '{table_name}': v1 (v0.19.0): {v1}; v3 (v0.20.0): {v3}; migration stopped"
    )]
    BothSchemaCandidatesFailed {
        table_name: String,
        v1: SchemaDecodeError,
        v3: SchemaDecodeError,
    },

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
pub enum SchemaDecodeError {
    #[error(transparent)]
    Payload(#[from] DecodeError),

    #[error("table name does not match the schema key")]
    TableNameMismatch,

    #[error("historical index access plan requires explicit recovery")]
    UnrecoverableIndexAccessPlan,
}

#[cfg(test)]
mod tests {
    use super::{DecodeError, MigrationError, SchemaDecodeError};

    #[test]
    fn schema_error_messages_include_the_table_and_typed_cause() {
        for (source, expected) in [
            (
                SchemaDecodeError::TableNameMismatch,
                "[RedbStorage] failed to decode schema in table 'Item': table name does not match the schema key; migration stopped",
            ),
            (
                SchemaDecodeError::Payload(DecodeError::NestingLimit),
                "[RedbStorage] failed to decode schema in table 'Item': migration decoding nesting limit exceeded; migration stopped",
            ),
            (
                SchemaDecodeError::UnrecoverableIndexAccessPlan,
                "[RedbStorage] failed to decode schema in table 'Item': historical index access plan requires explicit recovery; migration stopped",
            ),
        ] {
            let error = MigrationError::InvalidSchemaPayload {
                table_name: "Item".into(),
                source,
            };
            assert_eq!(error.to_string(), expected);
        }
    }

    #[test]
    fn both_candidate_failures_keep_their_version_labels_in_the_message() {
        let error = MigrationError::BothSchemaCandidatesFailed {
            table_name: "Item".into(),
            v1: SchemaDecodeError::Payload(DecodeError::Invalid(Box::new(
                bincode::ErrorKind::Custom("invalid field".into()),
            ))),
            v3: SchemaDecodeError::TableNameMismatch,
        };
        assert_eq!(
            error.to_string(),
            "[RedbStorage] failed to decode schema in table 'Item': v1 (v0.19.0): invalid field; v3 (v0.20.0): table name does not match the schema key; migration stopped"
        );
    }
}
