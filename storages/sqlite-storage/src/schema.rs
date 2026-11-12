use {
    crate::{Result, SqliteStorage, SqliteStorageError, codec::physical_type},
    gluesql_core::{
        ast::{ColumnDef, DataType, Expr, Literal, Statement, ToSql},
        data::{Schema, Value},
        parse_sql::{parse, parse_data_type, parse_expr},
        translate::{translate, translate_data_type, translate_expr},
    },
    rusqlite::{OptionalExtension, params},
    sqlparser::ast::{DataType as SqlDataType, Statement as SqlStatement},
    std::borrow::Cow,
};

const TABLE_META: &str = "__gluesql_table_meta";
const COLUMN_META: &str = "__gluesql_column_meta";

const TABLE_META_DDL: &str = "CREATE TABLE __gluesql_table_meta (
    table_name TEXT COLLATE NOCASE NOT NULL PRIMARY KEY,
    is_schemaless INTEGER NOT NULL DEFAULT 0 CHECK (is_schemaless IN (0, 1)),
    comment TEXT
) STRICT";

const COLUMN_META_DDL: &str = "CREATE TABLE __gluesql_column_meta (
    table_name TEXT COLLATE NOCASE NOT NULL,
    column_name TEXT COLLATE NOCASE NOT NULL,
    logical_type TEXT NOT NULL CHECK (logical_type IN (
        'BOOLEAN', 'INT8', 'INT16', 'INT32', 'INT', 'INT128',
        'UINT8', 'UINT16', 'UINT32', 'UINT64', 'UINT128',
        'FLOAT32', 'FLOAT', 'TEXT', 'BYTEA', 'INET', 'DATE',
        'TIMESTAMP', 'TIME', 'INTERVAL', 'UUID', 'MAP', 'LIST', 'DECIMAL', 'POINT')),
    comment TEXT,
    default_sql TEXT,
    PRIMARY KEY (table_name, column_name)
) STRICT";

pub(super) fn quote(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

pub(super) fn is_reserved(name: &str) -> bool {
    name.eq_ignore_ascii_case(TABLE_META) || name.eq_ignore_ascii_case(COLUMN_META)
}

pub(super) fn columns(schema: &Schema) -> Cow<'_, [ColumnDef]> {
    match &schema.column_defs {
        Some(columns) => Cow::Borrowed(columns),
        None => Cow::Owned(vec![ColumnDef {
            name: "_doc".to_owned(),
            data_type: DataType::Map,
            nullable: false,
            default: None,
            unique: None,
            comment: None,
        }]),
    }
}

pub(super) fn primary_key(columns: &[ColumnDef]) -> Option<usize> {
    columns
        .iter()
        .position(|column| column.unique.is_some_and(|unique| unique.is_primary))
}

pub(super) fn rowid_name(schema: &Schema) -> Result<&'static str> {
    let columns = columns(schema);
    ["_rowid_", "rowid", "oid"]
        .into_iter()
        .find(|alias| {
            !columns
                .iter()
                .any(|column| column.name.eq_ignore_ascii_case(alias))
        })
        .ok_or_else(|| SqliteStorageError::RowidUnavailable(schema.table_name.clone()))
}

fn physical_default(expr: &Expr) -> bool {
    match expr {
        Expr::Literal(Literal::QuotedString(value)) => !value.contains('\0'),
        Expr::Literal(Literal::Number(_)) | Expr::Value(Value::Null | Value::Bool(_)) => true,
        _ => false,
    }
}

impl SqliteStorage {
    pub(super) fn read_schema(&self, table_name: &str) -> Result<Option<Schema>> {
        if is_reserved(table_name) {
            return Ok(None);
        }

        let sql: Option<String> = self
            .connection
            .query_row(
                "SELECT sql
                 FROM sqlite_schema
                 WHERE type = 'table' AND name = ?1 COLLATE NOCASE",
                [table_name],
                |row| row.get(0),
            )
            .optional()?;
        let Some(sql) = sql else {
            return Ok(None);
        };

        let mut schema = parse_schema(sql.strip_suffix(" STRICT").unwrap_or(&sql))?;
        if !self.metadata_exists(COLUMN_META, COLUMN_META_DDL)? {
            return Err(SqliteStorageError::MissingLogicalType {
                table: table_name.to_owned(),
                column: columns(&schema)[0].name.clone(),
            });
        }

        let table_meta: Option<(bool, Option<String>)> =
            if self.metadata_exists(TABLE_META, TABLE_META_DDL)? {
                self.connection
                    .query_row(
                        "SELECT is_schemaless, comment
                         FROM __gluesql_table_meta
                         WHERE table_name = ?1",
                        [table_name],
                        |row| Ok((row.get(0)?, row.get(1)?)),
                    )
                    .optional()?
            } else {
                None
            };

        let mut column_meta = self.connection.prepare(
            "SELECT logical_type, comment, default_sql
             FROM __gluesql_column_meta
             WHERE table_name = ?1 AND column_name = ?2",
        )?;

        for column in schema.column_defs.iter_mut().flatten() {
            let meta: Option<(String, Option<String>, Option<String>)> = column_meta
                .query_row(params![table_name, column.name], |row| {
                    Ok((row.get(0)?, row.get(1)?, row.get(2)?))
                })
                .optional()?;
            let (logical_type, comment, default_sql) =
                meta.ok_or_else(|| SqliteStorageError::MissingLogicalType {
                    table: table_name.to_owned(),
                    column: column.name.clone(),
                })?;

            let data_type = parse_data_type(&logical_type)
                .and_then(|parsed| translate_data_type(&parsed))
                .map_err(|_| SqliteStorageError::InvalidLogicalType(logical_type.clone()))?;
            if data_type.to_string() != logical_type {
                return Err(SqliteStorageError::InvalidLogicalType(logical_type));
            }

            if physical_type(&data_type) != physical_type(&column.data_type) {
                return Err(SqliteStorageError::ConflictColumnType {
                    expected: data_type,
                    actual: column.data_type.to_string(),
                });
            }

            column.data_type = data_type;
            column.comment = comment;
            if let Some(sql) = default_sql {
                column.default = Some(translate_expr(&parse_expr(sql)?, &[])?);
            }
        }

        if let Some((schemaless, comment)) = table_meta {
            schema.comment = comment;

            if schemaless {
                let valid = schema.column_defs.as_ref().is_some_and(|columns| {
                    columns.len() == 1
                        && columns[0].name == "_doc"
                        && columns[0].data_type == DataType::Map
                });
                if !valid {
                    return Err(SqliteStorageError::ConflictSchemalessSchema(
                        schema.table_name,
                    ));
                }

                schema.column_defs = None;
            }
        }

        Ok(Some(schema))
    }

    pub(super) fn require_schema(&self, table_name: &str) -> Result<Schema> {
        self.read_schema(table_name)?
            .ok_or_else(|| SqliteStorageError::TableNotFound(table_name.to_owned()))
    }

    pub(super) fn write_schema(&self, schema: &Schema) -> Result<()> {
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
            self.connection.execute_batch(&create_table(schema))?;
        }

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

        self.connection.execute(
            "DELETE FROM __gluesql_column_meta WHERE table_name = ?1",
            [&schema.table_name],
        )?;

        for column in columns.iter() {
            let default = column
                .default
                .as_ref()
                .filter(|expr| !physical_default(expr))
                .map(ToSql::to_sql);

            self.connection.execute(
                "INSERT INTO __gluesql_column_meta
                     (table_name, column_name, logical_type, comment, default_sql)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                params![
                    schema.table_name,
                    column.name,
                    column.data_type.to_string(),
                    column.comment,
                    default,
                ],
            )?;
        }

        Ok(())
    }

    fn ensure_metadata(&self) -> Result<()> {
        for (name, ddl) in [(TABLE_META, TABLE_META_DDL), (COLUMN_META, COLUMN_META_DDL)] {
            if !self.metadata_exists(name, ddl)? {
                self.connection.execute_batch(ddl)?;
            }
        }

        Ok(())
    }

    fn metadata_exists(&self, name: &str, ddl: &str) -> Result<bool> {
        let stored: Option<String> = self
            .connection
            .query_row(
                "SELECT sql
                 FROM sqlite_schema
                 WHERE name = ?1 COLLATE NOCASE",
                [name],
                |row| row.get(0),
            )
            .optional()?;

        match stored {
            // This first format accepts only the metadata DDL emitted by this backend.
            Some(stored) if stored == ddl => Ok(true),
            Some(_) => Err(SqliteStorageError::ConflictMetadataSchema(name.to_owned())),
            None => Ok(false),
        }
    }

    fn rebuild_table(&self, old: &Schema, new: &Schema) -> Result<()> {
        let old_columns = columns(old);
        let new_columns = columns(new);
        let common = old_columns
            .iter()
            .filter(|old| new_columns.iter().any(|new| old.name == new.name))
            .map(|column| quote(&column.name))
            .collect::<Vec<_>>();

        let mut select = vec![quote(rowid_name(old)?)];
        select.extend(common.iter().cloned());
        let sql = format!(
            "SELECT {} FROM {} ORDER BY {}",
            select.join(", "),
            quote(&old.table_name),
            select[0]
        );

        let mut statement = self.connection.prepare(&sql)?;
        let saved = statement
            .query_map([], |row| {
                (0..select.len())
                    .map(|index| row.get::<_, rusqlite::types::Value>(index))
                    .collect::<rusqlite::Result<Vec<_>>>()
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        drop(statement);

        self.connection.execute_batch(&format!(
            "DROP TABLE {}; {}",
            quote(&old.table_name),
            create_table(new)
        ))?;

        let mut target = vec![quote(rowid_name(new)?)];
        target.extend(common);
        let parameters = vec!["?"; target.len()].join(", ");
        let sql = format!(
            "INSERT INTO {} ({}) VALUES ({parameters})",
            quote(&new.table_name),
            target.join(", ")
        );

        let mut statement = self.connection.prepare(&sql)?;
        for row in saved {
            statement.execute(rusqlite::params_from_iter(row))?;
        }

        Ok(())
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
}

fn create_table(schema: &Schema) -> String {
    let mut definitions = columns(schema)
        .iter()
        .map(|column| {
            let primary = column.unique.is_some_and(|unique| unique.is_primary);
            // INT avoids the INTEGER PRIMARY KEY rowid alias and preserves insertion order.
            let physical = match (physical_type(&column.data_type), primary) {
                ("INTEGER", true) => "INT",
                (physical, _) => physical,
            };

            let mut sql = format!("{} {physical}", quote(&column.name));
            if !column.nullable {
                sql.push_str(" NOT NULL");
            }

            if let Some(unique) = column.unique {
                sql.push_str(if unique.is_primary {
                    " PRIMARY KEY"
                } else {
                    " UNIQUE"
                });
            }

            if let Some(default) = column
                .default
                .as_ref()
                .filter(|expr| physical_default(expr))
            {
                sql.push_str(" DEFAULT ");
                sql.push_str(&default.to_sql());
            }

            sql
        })
        .collect::<Vec<_>>();

    definitions.extend(schema.foreign_keys.iter().map(|key| {
        format!(
            "CONSTRAINT {} FOREIGN KEY ({}) REFERENCES {} ({}) ON DELETE {} ON UPDATE {}",
            quote(&key.name),
            quote(&key.referencing_column_name),
            quote(&key.referenced_table_name),
            quote(&key.referenced_column_name),
            key.on_delete,
            key.on_update,
        )
    }));

    format!(
        "CREATE TABLE {} ({}) STRICT",
        quote(&schema.table_name),
        definitions.join(", ")
    )
}

fn parse_schema(sql: &str) -> Result<Schema> {
    let mut parsed = parse(sql)?;
    let Some(statement @ SqlStatement::CreateTable(_)) = parsed.first_mut() else {
        return Err(SqliteStorageError::InvalidTableDefinition);
    };

    if let SqlStatement::CreateTable(table) = statement {
        for column in &mut table.columns {
            column.data_type = match &column.data_type {
                SqlDataType::Real => SqlDataType::Float(None),
                SqlDataType::Blob(_) => SqlDataType::Bytea,
                other => other.clone(),
            };
        }
    }

    let Statement::CreateTable {
        name,
        columns,
        engine,
        foreign_keys,
        comment,
        ..
    } = translate(statement)?
    else {
        // translate maps the matched SqlStatement::CreateTable only to CreateTable.
        return Err(SqliteStorageError::UnreachableCreateTableTranslation);
    };

    Ok(Schema {
        table_name: name,
        column_defs: columns,
        engine,
        foreign_keys,
        comment,
        indexes: Vec::new(),
    })
}

#[cfg(test)]
mod tests {
    use {super::parse_schema, crate::SqliteStorageError};

    #[test]
    fn rejects_definitions_without_a_table() {
        for sql in ["", "SELECT 1"] {
            assert!(matches!(
                parse_schema(sql),
                Err(SqliteStorageError::InvalidTableDefinition)
            ));
        }
    }
}
