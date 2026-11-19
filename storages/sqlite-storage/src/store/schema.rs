use {
    crate::{
        Result, SqliteStorage, SqliteStorageError,
        schema::{
            COLUMN_META, COLUMN_META_DDL, ColumnMetadata, TABLE_META, TABLE_META_DDL,
            TableMetadata, columns, is_reserved, parse_physical_schema,
        },
    },
    gluesql_core::data::Schema,
    rusqlite::{OptionalExtension, params},
};

impl SqliteStorage {
    pub(crate) fn read_schema(&self, table_name: &str) -> Result<Option<Schema>> {
        if is_reserved(table_name) {
            return Ok(None);
        }

        let sql: Option<String> = self
            .connection
            .query_row(
                "SELECT sql
                 FROM sqlite_schema
                 WHERE type = 'table' AND name = ?1",
                [table_name],
                |row| row.get(0),
            )
            .optional()?;
        let Some(sql) = sql else {
            return Ok(None);
        };

        let mut schema = parse_physical_schema(&sql)?;
        if !self.metadata_exists(COLUMN_META)? {
            return Err(SqliteStorageError::MissingLogicalType {
                table: table_name.to_owned(),
                column: columns(&schema)[0].name.clone(),
            });
        }

        let table_metadata = self.read_table_metadata(table_name)?;

        let mut statement = self.connection.prepare(
            "SELECT logical_type, comment, default_sql
             FROM __gluesql_column_meta
             WHERE table_name = ?1 AND column_name = ?2",
        )?;

        for column in schema.column_defs.iter_mut().flatten() {
            let metadata = statement
                .query_row(params![table_name, column.name], |row| {
                    Ok(ColumnMetadata {
                        logical_type: row.get(0)?,
                        comment: row.get(1)?,
                        default_sql: row.get(2)?,
                    })
                })
                .optional()?
                .ok_or_else(|| SqliteStorageError::MissingLogicalType {
                    table: table_name.to_owned(),
                    column: column.name.clone(),
                })?;

            metadata.restore_column(column)?;
        }

        if let Some(metadata) = table_metadata {
            metadata.restore_table(&mut schema)?;
        }

        Ok(Some(schema))
    }

    pub(crate) fn require_schema(&self, table_name: &str) -> Result<Schema> {
        self.read_schema(table_name)?
            .ok_or_else(|| SqliteStorageError::TableNotFound(table_name.to_owned()))
    }

    pub(super) fn read_all_schemas(&self) -> Result<Vec<Schema>> {
        let mut statement = self.connection.prepare(
            "SELECT name
             FROM sqlite_schema
             WHERE type = 'table' AND name NOT GLOB 'sqlite_*'
             ORDER BY name",
        )?;
        let names = statement
            .query_map([], |row| row.get::<_, String>(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?;

        names
            .into_iter()
            .filter(|name| !is_reserved(name))
            .map(|name| self.require_schema(&name))
            .collect()
    }

    fn read_table_metadata(&self, table_name: &str) -> Result<Option<TableMetadata>> {
        if !self.metadata_exists(TABLE_META)? {
            return Ok(None);
        }

        let metadata = self
            .connection
            .query_row(
                "SELECT is_schemaless, comment
                 FROM __gluesql_table_meta
                 WHERE table_name = ?1",
                [table_name],
                |row| {
                    Ok(TableMetadata {
                        is_schemaless: row.get(0)?,
                        comment: row.get(1)?,
                    })
                },
            )
            .optional()?;

        Ok(metadata)
    }

    fn metadata_exists(&self, name: &str) -> Result<bool> {
        let exists = self.connection.query_row(
            "SELECT EXISTS (
                 SELECT 1 FROM sqlite_schema WHERE name = ?1 COLLATE NOCASE
             )",
            [name],
            |row| row.get(0),
        )?;

        Ok(exists)
    }

    pub(crate) fn validate_metadata(&self) -> Result<()> {
        for (name, ddl) in [(TABLE_META, TABLE_META_DDL), (COLUMN_META, COLUMN_META_DDL)] {
            if let Some(stored) = self.read_metadata_ddl(name)?
                && stored != ddl.trim()
            {
                return Err(SqliteStorageError::ConflictMetadataSchema(name.to_owned()));
            }
        }

        Ok(())
    }

    pub(crate) fn read_metadata_ddl(&self, name: &str) -> Result<Option<String>> {
        let ddl = self
            .connection
            .query_row(
                "SELECT sql
                 FROM sqlite_schema
                 WHERE name = ?1 COLLATE NOCASE",
                [name],
                |row| row.get(0),
            )
            .optional()?;

        Ok(ddl)
    }
}
