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

#[cfg(test)]
mod tests {
    use {
        super::{MigrationReport, SCHEMA_TABLE, SchemaRewrites},
        crate::{error::StorageError, migration::MigrationError},
        redb::{Database, TableDefinition, TableError, backends::InMemoryBackend},
    };

    #[test]
    fn missing_schema_table_has_nothing_to_apply() {
        let db = Database::builder()
            .create_with_backend(InMemoryBackend::new())
            .unwrap();
        let schemas = SchemaRewrites::inspect(&db, |_, _| Ok(None)).unwrap();
        assert!(schemas.table_names().is_empty());
        assert!(schemas.rewrites.is_empty());

        let txn = db.begin_write().unwrap();
        assert_eq!(schemas.apply(&txn).unwrap(), MigrationReport::default());
        txn.commit().unwrap();
        assert!(matches!(
            db.begin_read().unwrap().open_table(SCHEMA_TABLE),
            Err(TableError::TableDoesNotExist(name)) if name == "__SCHEMA__"
        ));
    }

    #[test]
    fn applies_only_changed_payloads_and_counts_both_outcomes() {
        let db = Database::builder()
            .create_with_backend(InMemoryBackend::new())
            .unwrap();
        let txn = db.begin_write().unwrap();
        {
            let mut table = txn.open_table(SCHEMA_TABLE).unwrap();
            table.insert("ZUnchanged", b"current".to_vec()).unwrap();
            table.insert("AChanged", b"old".to_vec()).unwrap();
        }
        txn.commit().unwrap();

        let schemas = SchemaRewrites::inspect(&db, |_, payload| {
            Ok((payload == b"old").then(|| b"restored".to_vec()))
        })
        .unwrap();
        assert_eq!(schemas.table_names(), &["AChanged", "ZUnchanged"]);
        assert_eq!(
            schemas.rewrites,
            vec![("AChanged".into(), b"restored".to_vec())]
        );
        let txn = db.begin_write().unwrap();
        assert_eq!(
            schemas.apply(&txn).unwrap(),
            MigrationReport {
                migrated_tables: 1,
                unchanged_tables: 1,
                rewritten_rows: 0,
            }
        );
        txn.commit().unwrap();
        let txn = db.begin_read().unwrap();
        let table = txn.open_table(SCHEMA_TABLE).unwrap();
        assert_eq!(table.get("AChanged").unwrap().unwrap().value(), b"restored");
        assert_eq!(
            table.get("ZUnchanged").unwrap().unwrap().value(),
            b"current"
        );
    }

    #[test]
    fn callback_failure_preserves_its_error_and_does_not_apply_earlier_rewrites() {
        let db = Database::builder()
            .create_with_backend(InMemoryBackend::new())
            .unwrap();
        let txn = db.begin_write().unwrap();
        {
            let mut table = txn.open_table(SCHEMA_TABLE).unwrap();
            table.insert("AChanged", b"old".to_vec()).unwrap();
            table.insert("ZBroken", b"ambiguous".to_vec()).unwrap();
        }
        txn.commit().unwrap();

        let result = SchemaRewrites::inspect(&db, |name, payload| {
            if payload == b"ambiguous" {
                Err(MigrationError::AmbiguousSchemaPayload(name.to_owned()).into())
            } else {
                Ok(Some(b"restored".to_vec()))
            }
        });
        assert!(matches!(result,
            Err(StorageError::Migration(MigrationError::AmbiguousSchemaPayload(name)))
            if name == "ZBroken"
        ));
        let txn = db.begin_read().unwrap();
        let table = txn.open_table(SCHEMA_TABLE).unwrap();
        assert_eq!(table.get("AChanged").unwrap().unwrap().value(), b"old");
        assert_eq!(table.get("ZBroken").unwrap().unwrap().value(), b"ambiguous");
    }

    #[test]
    fn schema_table_type_errors_are_not_treated_as_an_empty_database() {
        let db = Database::builder()
            .create_with_backend(InMemoryBackend::new())
            .unwrap();
        let txn = db.begin_write().unwrap();
        txn.open_table(TableDefinition::<&str, u32>::new("__SCHEMA__"))
            .unwrap();
        txn.commit().unwrap();

        assert!(matches!(SchemaRewrites::inspect(&db, |_, _| Ok(None)),
            Err(StorageError::RedbTable(TableError::TableTypeMismatch { table, key, value }))
            if table == "__SCHEMA__"
                && key == <&str as redb::Value>::type_name()
                && value == <u32 as redb::Value>::type_name()
        ));
    }
}
