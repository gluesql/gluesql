//! Read schema tables and apply changes prepared by each migration path.

use {
    super::{MigrationReport, SCHEMA_TABLE, StorageResult},
    redb::{Database, ReadableTable, WriteTransaction},
};

type RewriteSchema = fn(&str, &[u8]) -> StorageResult<Option<Vec<u8>>>;

pub(super) struct SchemaRewrites {
    table_names: Vec<String>,
    rewrites: Vec<(String, Vec<u8>)>,
}

impl SchemaRewrites {
    pub(super) fn inspect(db: &Database, rewrite_schema: RewriteSchema) -> StorageResult<Self> {
        let txn = db.begin_read()?;
        let table = match txn.open_table(SCHEMA_TABLE) {
            Ok(table) => table,
            Err(redb::TableError::TableDoesNotExist(_)) => {
                return Ok(Self {
                    table_names: Vec::new(),
                    rewrites: Vec::new(),
                });
            }
            Err(error) => return Err(error.into()),
        };
        let mut migration = Self {
            table_names: Vec::new(),
            rewrites: Vec::new(),
        };
        for entry in table.iter()? {
            let (name, payload) = entry?;
            let name = name.value().to_owned();
            if let Some(payload) = rewrite_schema(&name, &payload.value())? {
                migration.rewrites.push((name.clone(), payload));
            }
            migration.table_names.push(name);
        }
        migration.table_names.sort();
        Ok(migration)
    }

    pub(super) fn table_names(&self) -> &[String] {
        &self.table_names
    }

    pub(super) fn apply(self, txn: &WriteTransaction) -> StorageResult<MigrationReport> {
        let report = MigrationReport {
            migrated_tables: self.rewrites.len(),
            unchanged_tables: self.table_names.len() - self.rewrites.len(),
            rewritten_rows: 0,
        };
        if !self.rewrites.is_empty() {
            let mut table = txn.open_table(SCHEMA_TABLE)?;
            for (name, payload) in self.rewrites {
                table.insert(name.as_str(), payload)?;
            }
        }
        Ok(report)
    }
}
