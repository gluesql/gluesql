use {
    crate::{SqliteStorage, SqliteStorageError},
    gluesql_core::{error::Result, store::Transaction},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum TransactionState {
    Idle,
    Automatic,
    Explicit,
}

impl Transaction for SqliteStorage {
    fn begin(&mut self, autocommit: bool) -> Result<bool> {
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

    fn commit(&mut self) -> Result<()> {
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

    fn rollback(&mut self) -> Result<()> {
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
