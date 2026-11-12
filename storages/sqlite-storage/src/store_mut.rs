use {
    crate::{
        Result, SqliteStorage, SqliteStorageError,
        codec::encode,
        schema::{columns, primary_key, quote, rowid_name},
    },
    gluesql_core::{
        data::{Key, Schema, Value},
        error::Result as GlueResult,
        store::{ScanColumns, StoreMut},
    },
    rusqlite::{params_from_iter, types::Value as SqlValue},
};

impl StoreMut for SqliteStorage {
    fn insert_schema(&mut self, schema: &Schema) -> GlueResult<()> {
        self.atomic(|storage| storage.write_schema(schema))
            .map_err(Into::into)
    }

    fn delete_schema(&mut self, table_name: &str) -> GlueResult<()> {
        self.atomic(|storage| storage.remove_schema(table_name))
            .map_err(Into::into)
    }

    fn append_data(&mut self, table_name: &str, rows: Vec<Vec<Value>>) -> GlueResult<()> {
        self.atomic(|storage| storage.append_rows(table_name, &rows))
            .map_err(Into::into)
    }

    fn insert_data(&mut self, table_name: &str, rows: Vec<(Key, Vec<Value>)>) -> GlueResult<()> {
        self.atomic(|storage| storage.write_rows(table_name, &rows))
            .map_err(Into::into)
    }

    fn delete_data(&mut self, table_name: &str, keys: Vec<Key>) -> GlueResult<()> {
        self.atomic(|storage| storage.delete_rows(table_name, &keys))
            .map_err(Into::into)
    }
}

impl SqliteStorage {
    fn atomic(&self, operation: impl FnOnce(&Self) -> Result<()>) -> Result<()> {
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

    fn append_rows(&self, table_name: &str, rows: &[Vec<Value>]) -> Result<()> {
        let schema = self.require_schema(table_name)?;
        let columns = columns(&schema);

        let names = columns
            .iter()
            .map(|column| quote(&column.name))
            .collect::<Vec<_>>()
            .join(", ");
        let parameters = vec!["?"; columns.len()].join(", ");
        let sql = format!(
            "INSERT INTO {} ({names}) VALUES ({parameters})",
            quote(table_name)
        );

        let mut statement = self.connection.prepare(&sql)?;
        for row in rows {
            let values = encode_row(row, &columns)?;
            statement.execute(params_from_iter(values))?;
        }

        Ok(())
    }

    fn write_rows(&self, table_name: &str, rows: &[(Key, Vec<Value>)]) -> Result<()> {
        let schema = self.require_schema(table_name)?;

        for (key, row) in rows {
            self.write_row(&schema, key, row)?;
        }

        Ok(())
    }

    fn write_row(&self, schema: &Schema, key: &Key, row: &[Value]) -> Result<()> {
        let columns = columns(schema);
        let mut values = encode_row(row, &columns)?;
        let primary = primary_key(&columns);
        if let Some(index) = primary {
            let actual = Key::try_from(&row[index])?;
            if &actual != key {
                return Err(SqliteStorageError::ConflictPrimaryKey {
                    expected: key.clone(),
                    actual,
                });
            }
        }

        let existing = self
            .read_rows(schema, ScanColumns::All)?
            .into_iter()
            .find(|row| &row.key == key);

        if let Some(existing) = existing {
            let assignments = columns
                .iter()
                .map(|column| format!("{} = ?", quote(&column.name)))
                .collect::<Vec<_>>()
                .join(", ");
            values.push(SqlValue::Integer(existing.rowid));

            let sql = format!(
                "UPDATE {} SET {assignments} WHERE {} = ?",
                quote(&schema.table_name),
                quote(rowid_name(schema)?)
            );
            self.connection.execute(&sql, params_from_iter(values))?;
        } else {
            let mut names = columns
                .iter()
                .map(|column| quote(&column.name))
                .collect::<Vec<_>>();

            if primary.is_none() {
                let Key::I64(rowid) = key else {
                    return Err(SqliteStorageError::InvalidRowidKey(key.clone()));
                };
                names.push(quote(rowid_name(schema)?));
                values.push(SqlValue::Integer(*rowid));
            }

            let placeholders = vec!["?"; names.len()].join(", ");
            let sql = format!(
                "INSERT INTO {} ({}) VALUES ({placeholders})",
                quote(&schema.table_name),
                names.join(", ")
            );
            self.connection.execute(&sql, params_from_iter(values))?;
        }

        Ok(())
    }

    fn delete_rows(&self, table_name: &str, keys: &[Key]) -> Result<()> {
        let schema = self.require_schema(table_name)?;
        let rows = self.read_rows(&schema, ScanColumns::All)?;

        let sql = format!(
            "DELETE FROM {} WHERE {} = ?1",
            quote(table_name),
            quote(rowid_name(&schema)?)
        );
        let mut statement = self.connection.prepare(&sql)?;

        for row in rows {
            if keys.contains(&row.key) {
                statement.execute([row.rowid])?;
            }
        }

        Ok(())
    }
}

fn encode_row(row: &[Value], columns: &[gluesql_core::ast::ColumnDef]) -> Result<Vec<SqlValue>> {
    if row.len() != columns.len() {
        return Err(SqliteStorageError::ConflictRowLength {
            expected: columns.len(),
            actual: row.len(),
        });
    }

    row.iter()
        .zip(columns)
        .map(|(value, column)| encode(value, &column.data_type))
        .collect()
}
