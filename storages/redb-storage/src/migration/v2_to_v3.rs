use {crate::error::StorageError, redb::Database};

type StorageResult<T> = std::result::Result<T, StorageError>;

// redb 2.6 supports upgrading its internal file format from v2 to v3.
// GlueSQL's row serialization format is unchanged between storage format v2 and v3.
pub(super) fn migrate(db: &mut Database) -> StorageResult<()> {
    db.upgrade()?;
    Ok(())
}
