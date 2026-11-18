mod schema;

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
        if rows.is_empty() {
            return Ok(());
        }

        let columns = columns(&schema);
        let mut names = columns
            .iter()
            .map(|column| quote(&column.name))
            .collect::<Vec<_>>();
        let assignments = names
            .iter()
            .map(|name| format!("{name} = excluded.{name}"))
            .collect::<Vec<_>>()
            .join(", ");

        let primary = primary_key(&columns);
        let conflict_column = if let Some(index) = primary {
            names[index].clone()
        } else {
            let rowid = quote(rowid_name(&schema)?);
            names.push(rowid.clone());

            rowid
        };
        let placeholders = vec!["?"; names.len()].join(", ");
        let sql = format!(
            "INSERT INTO {} ({}) VALUES ({placeholders})
             ON CONFLICT ({conflict_column}) DO UPDATE SET {assignments}",
            quote(table_name),
            names.join(", ")
        );
        let mut statement = self.connection.prepare(&sql)?;

        for (key, row) in rows {
            let mut values = encode_row(row, &columns)?;
            if let Some(index) = primary {
                validate_primary_key(key, &row[index])?;
            } else {
                let Key::I64(rowid) = key else {
                    return Err(SqliteStorageError::InvalidRowidKey(key.clone()));
                };

                values.push(SqlValue::Integer(*rowid));
            }

            statement.execute(params_from_iter(values))?;
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

fn validate_primary_key(expected: &Key, value: &Value) -> Result<()> {
    let actual = Key::try_from(value)?;
    if &actual != expected {
        return Err(SqliteStorageError::ConflictPrimaryKey {
            expected: expected.clone(),
            actual,
        });
    }

    Ok(())
}
