//! v4 -> v5 reserves indexed databases for writers that maintain their indexes.
//! Schema and row payloads are unchanged.

use {
    super::{
        MigrationReport, StorageResult, V5_REDB_STORAGE_FORMAT_VERSION,
        list_user_table_names_from_read, write_storage_format_version,
    },
    redb::Database,
};

pub(super) fn migrate(db: &Database) -> StorageResult<MigrationReport> {
    let unchanged_tables = list_user_table_names_from_read(&db.begin_read()?)?.len();
    let txn = db.begin_write()?;
    write_storage_format_version(&txn, V5_REDB_STORAGE_FORMAT_VERSION)?;
    txn.commit()?;
    Ok(MigrationReport {
        migrated_tables: 0,
        unchanged_tables,
        rewritten_rows: 0,
    })
}
