use {
    crate::{Result, SqliteStorage, SqliteStorageError},
    gluesql_core::{error::Result as GlueResult, store::Transaction},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum TransactionState {
    Idle,
    Automatic,
    Explicit,
}

impl Transaction for SqliteStorage {
    fn begin(&mut self, autocommit: bool) -> GlueResult<bool> {
        if !self.connection.is_autocommit() {
            return if autocommit {
                Ok(false)
            } else {
                Err(SqliteStorageError::NestedTransaction.into())
            };
        }

        self.connection
            .execute_batch("BEGIN DEFERRED")
            .map_err(SqliteStorageError::from)?;
        self.transaction = if autocommit {
            TransactionState::Automatic
        } else {
            TransactionState::Explicit
        };

        Ok(true)
    }

    fn commit(&mut self) -> GlueResult<()> {
        if self.connection.is_autocommit() {
            self.transaction = TransactionState::Idle;

            return Err(SqliteStorageError::TransactionNotFound.into());
        }

        match self.connection.execute_batch("COMMIT") {
            Ok(()) => {
                self.transaction = TransactionState::Idle;

                Ok(())
            }
            Err(commit) => {
                if self.transaction == TransactionState::Automatic
                    && !self.connection.is_autocommit()
                    && let Err(rollback) = self.connection.execute_batch("ROLLBACK")
                {
                    return Err(SqliteStorageError::AutoCommitRollback {
                        commit: Box::new(commit),
                        rollback,
                    }
                    .into());
                }

                if self.connection.is_autocommit() {
                    self.transaction = TransactionState::Idle;
                }

                Err(SqliteStorageError::Sqlite(commit).into())
            }
        }
    }

    fn rollback(&mut self) -> GlueResult<()> {
        if self.connection.is_autocommit() {
            let previous = std::mem::replace(&mut self.transaction, TransactionState::Idle);

            return if previous == TransactionState::Idle {
                Err(SqliteStorageError::TransactionNotFound.into())
            } else {
                // SQLite can itself roll back a transaction after a database failure.
                Ok(())
            };
        }

        self.connection
            .execute_batch("ROLLBACK")
            .map_err(SqliteStorageError::from)?;
        self.transaction = TransactionState::Idle;

        Ok(())
    }
}

impl SqliteStorage {
    pub(super) fn atomic(&self, operation: impl FnOnce(&Self) -> Result<()>) -> Result<()> {
        let outermost = self.connection.is_autocommit();
        self.connection.execute_batch("SAVEPOINT __gluesql_write")?;

        let result = operation(self).and_then(|()| {
            self.connection
                .execute_batch("RELEASE __gluesql_write")
                .map_err(Into::into)
        });

        match result {
            Err(operation) if !self.connection.is_autocommit() => {
                let sql = if outermost {
                    "ROLLBACK"
                } else {
                    "ROLLBACK TO __gluesql_write; RELEASE __gluesql_write"
                };

                if let Err(rollback) = self.connection.execute_batch(sql) {
                    return Err(SqliteStorageError::SavepointRollback {
                        operation: Box::new(operation),
                        rollback,
                    });
                }

                Err(operation)
            }
            result => result,
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        crate::{SqliteStorage, SqliteStorageError},
        gluesql_core::{
            data::Schema,
            store::{Store, StoreMut, Transaction},
        },
        rusqlite::{Connection, Error as SqliteError, ffi},
    };

    #[test]
    fn atomic_reports_both_errors_when_savepoint_rollback_fails() {
        let uri = "file:savepoint_rollback_lock?mode=memory&cache=shared";
        let mut storage = SqliteStorage::new(uri).unwrap();
        let writer = Connection::open(uri).unwrap();
        let reserved = Schema::from_ddl("CREATE TABLE __gluesql_table_meta (id INT)").unwrap();
        storage.begin(false).unwrap();

        let error = storage
            .atomic(|storage| {
                // Take the schema lock after the savepoint opens so it blocks the rollback.
                writer
                    .execute_batch("BEGIN; CREATE TABLE pending (id INTEGER)")
                    .unwrap();
                storage.write_schema(&reserved)
            })
            .unwrap_err();
        let SqliteStorageError::SavepointRollback {
            operation,
            rollback,
        } = error
        else {
            panic!("expected SavepointRollback, got {error:?}");
        };

        assert!(
            matches!(
                operation.as_ref(),
                SqliteStorageError::ReservedTableName(name) if name == "__gluesql_table_meta"
            ),
            "{operation:?}"
        );

        let expected = SqliteError::SqliteFailure(
            ffi::Error::new(ffi::SQLITE_LOCKED_SHAREDCACHE),
            Some("database schema is locked: main".to_owned()),
        );
        assert_eq!(rollback, expected);

        writer.execute_batch("ROLLBACK").unwrap();
        storage.rollback().unwrap();

        let schema = Schema::from_ddl("CREATE TABLE item (id INT)").unwrap();
        storage.insert_schema(&schema).unwrap();
        assert_eq!(storage.fetch_schema("item").unwrap(), Some(schema));
    }
}
