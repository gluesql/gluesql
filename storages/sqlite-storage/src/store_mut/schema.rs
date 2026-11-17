use {
    crate::{
        Result, SqliteStorage, SqliteStorageError,
        schema::{
            COLUMN_META, COLUMN_META_DDL, TABLE_META, TABLE_META_DDL, columns, create_table_sql,
            is_reserved, physical_default, quote, rowid_name,
        },
    },
    gluesql_core::{
        ast::{ColumnDef, ToSql},
        data::Schema,
    },
    rusqlite::{params, types::Value as SqlValue},
};

impl SqliteStorage {
    pub(crate) fn write_schema(&self, schema: &Schema) -> Result<()> {
        if is_reserved(&schema.table_name) {
            return Err(SqliteStorageError::ReservedTableName(
                schema.table_name.clone(),
            ));
        }

        if !schema.indexes.is_empty() {
            return Err(SqliteStorageError::SchemaIndexesNotSupported);
        }

        if let Some(engine) = &schema.engine {
            return Err(SqliteStorageError::EngineNotSupported(engine.clone()));
        }

        let columns = columns(schema);
        if columns.is_empty() {
            return Err(SqliteStorageError::EmptySchema(schema.table_name.clone()));
        }

        rowid_name(schema)?;

        let old = self.read_schema(&schema.table_name)?;
        if let Some(old) = &old
            && old.column_defs.is_none() != schema.column_defs.is_none()
        {
            return Err(SqliteStorageError::SchemaKindChangeNotSupported(
                schema.table_name.clone(),
            ));
        }

        self.ensure_metadata()?;

        if let Some(old) = &old {
            self.rebuild_table(old, schema)?;
        } else {
            self.connection.execute_batch(&create_table_sql(schema))?;
        }

        self.write_table_metadata(schema)?;
        self.write_column_metadata(&schema.table_name, &columns)
    }

    pub(super) fn remove_schema(&self, table_name: &str) -> Result<()> {
        if is_reserved(table_name) {
            return Err(SqliteStorageError::ReservedTableName(table_name.to_owned()));
        }

        if self.read_schema(table_name)?.is_none() {
            return Ok(());
        }

        self.connection
            .execute_batch(&format!("DROP TABLE {}", quote(table_name)))?;

        self.connection.execute(
            "DELETE FROM __gluesql_column_meta WHERE table_name = ?1",
            [table_name],
        )?;
        self.connection.execute(
            "DELETE FROM __gluesql_table_meta WHERE table_name = ?1",
            [table_name],
        )?;

        Ok(())
    }

    fn ensure_metadata(&self) -> Result<()> {
        for (name, ddl) in [(TABLE_META, TABLE_META_DDL), (COLUMN_META, COLUMN_META_DDL)] {
            let ddl = ddl.trim();
            match self.read_metadata_ddl(name)? {
                Some(stored) if stored != ddl => {
                    return Err(SqliteStorageError::ConflictMetadataSchema(name.to_owned()));
                }
                Some(_) => {}
                None => self.connection.execute_batch(ddl)?,
            }
        }

        Ok(())
    }

    fn rebuild_table(&self, old: &Schema, new: &Schema) -> Result<()> {
        let old_columns = columns(old);
        let new_columns = columns(new);
        let common = old_columns
            .iter()
            .filter(|old| new_columns.iter().any(|new| old.name == new.name))
            .map(|column| column.name.clone())
            .collect::<Vec<_>>();

        let rows = self.read_rows_for_rebuild(old, &common)?;
        self.recreate_table(new, &common, &rows)
    }

    pub(crate) fn read_rows_for_rebuild(
        &self,
        schema: &Schema,
        columns: &[String],
    ) -> Result<Vec<Vec<SqlValue>>> {
        // Keep the physical rowid first, followed by unchanged SQLite values.
        let mut select = vec![quote(rowid_name(schema)?)];
        select.extend(columns.iter().map(|name| quote(name)));
        let sql = format!(
            "SELECT {} FROM {} ORDER BY {}",
            select.join(", "),
            quote(&schema.table_name),
            select[0]
        );

        let mut statement = self.connection.prepare(&sql)?;
        let rows = statement
            .query_map([], |row| {
                (0..select.len())
                    .map(|index| row.get::<_, SqlValue>(index))
                    .collect::<rusqlite::Result<Vec<_>>>()
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;

        Ok(rows)
    }

    pub(crate) fn recreate_table(
        &self,
        schema: &Schema,
        columns: &[String],
        rows: &[Vec<SqlValue>],
    ) -> Result<()> {
        let deferred: bool =
            self.connection
                .pragma_query_value(None, "defer_foreign_keys", |row| row.get(0))?;
        self.connection
            .pragma_update(None, "defer_foreign_keys", true)?;

        let result = self
            .connection
            .execute_batch(&format!(
                "DROP TABLE {}; {}",
                quote(&schema.table_name),
                create_table_sql(schema)
            ))
            .map_err(Into::into)
            .and_then(|()| self.insert_rows_for_rebuild(schema, columns, rows))
            .and_then(|()| self.check_foreign_keys());

        // Disabling deferral clears SQLite's violation tracking. Check first;
        // on failure, the caller's atomic operation rolls back the rebuild.
        let restored = self
            .connection
            .pragma_update(None, "defer_foreign_keys", deferred);

        match (result, restored) {
            (result, Ok(())) => result,
            (Ok(()), Err(error)) => Err(error.into()),
            (Err(operation), Err(restore)) => Err(SqliteStorageError::ForeignKeyDeferralRestore {
                operation: Box::new(operation),
                restore,
            }),
        }
    }

    fn insert_rows_for_rebuild(
        &self,
        schema: &Schema,
        columns: &[String],
        rows: &[Vec<SqlValue>],
    ) -> Result<()> {
        let mut target = vec![quote(rowid_name(schema)?)];
        target.extend(columns.iter().map(|name| quote(name)));
        let parameters = vec!["?"; target.len()].join(", ");
        let sql = format!(
            "INSERT INTO {} ({}) VALUES ({parameters})",
            quote(&schema.table_name),
            target.join(", ")
        );

        let mut statement = self.connection.prepare(&sql)?;
        for row in rows {
            statement.execute(rusqlite::params_from_iter(row))?;
        }

        Ok(())
    }

    fn check_foreign_keys(&self) -> Result<()> {
        // Checking only the rebuilt table would miss references from its children.
        let mut statement = self.connection.prepare("PRAGMA foreign_key_check")?;
        let mut rows = statement.query([])?;
        if let Some(row) = rows.next()? {
            return Err(SqliteStorageError::ConflictForeignKey {
                table: row.get(0)?,
                rowid: row.get(1)?,
                parent: row.get(2)?,
            });
        }

        Ok(())
    }

    fn write_table_metadata(&self, schema: &Schema) -> Result<()> {
        if schema.column_defs.is_none() || schema.comment.is_some() {
            self.connection.execute(
                "INSERT INTO __gluesql_table_meta (table_name, is_schemaless, comment)
                 VALUES (?1, ?2, ?3)
                 ON CONFLICT(table_name) DO UPDATE
                 SET is_schemaless = excluded.is_schemaless, comment = excluded.comment",
                params![
                    schema.table_name,
                    schema.column_defs.is_none(),
                    schema.comment
                ],
            )?;
        } else {
            self.connection.execute(
                "DELETE FROM __gluesql_table_meta WHERE table_name = ?1",
                [&schema.table_name],
            )?;
        }

        Ok(())
    }

    pub(crate) fn write_column_metadata(
        &self,
        table_name: &str,
        columns: &[ColumnDef],
    ) -> Result<()> {
        self.connection.execute(
            "DELETE FROM __gluesql_column_meta WHERE table_name = ?1",
            [table_name],
        )?;

        for column in columns {
            let default_sql = column
                .default
                .as_ref()
                .filter(|expr| !physical_default(expr))
                .map(ToSql::to_sql);

            self.connection.execute(
                "INSERT INTO __gluesql_column_meta
                     (table_name, column_name, logical_type, comment, default_sql)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                params![
                    table_name,
                    column.name,
                    column.data_type.to_string(),
                    column.comment,
                    default_sql,
                ],
            )?;
        }

        Ok(())
    }
}
