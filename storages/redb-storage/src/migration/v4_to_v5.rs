//! v4 -> v5 reserves indexed databases for writers that maintain their indexes.
//! Schema and row payloads are unchanged.

use {
    super::{
        MigrationError, MigrationReport, StorageResult, V5_REDB_STORAGE_FORMAT_VERSION,
        list_user_table_names_from_read, write_storage_format_version,
    },
    redb::Database,
};

pub(super) fn migrate(db: &Database) -> StorageResult<MigrationReport> {
    let table_names = list_user_table_names_from_read(&db.begin_read()?)?;

    for table_name in &table_names {
        if table_name.contains('/') {
            return Err(MigrationError::V5TableNameContainsSlash(table_name.clone()).into());
        }
    }

    let txn = db.begin_write()?;
    write_storage_format_version(&txn, V5_REDB_STORAGE_FORMAT_VERSION)?;
    txn.commit()?;

    Ok(MigrationReport {
        migrated_tables: 0,
        unchanged_tables: table_names.len(),
        rewritten_rows: 0,
    })
}
