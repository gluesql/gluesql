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

const TABLE_META_DDL: &str = "
CREATE TABLE __gluesql_table_meta (
    table_name TEXT COLLATE NOCASE NOT NULL PRIMARY KEY,
    is_schemaless INTEGER NOT NULL DEFAULT 0 CHECK (is_schemaless IN (0, 1)),
    comment TEXT
) STRICT
";

const COLUMN_META_DDL: &str = "
CREATE TABLE __gluesql_column_meta (
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
) STRICT
";

impl SqliteStorage {
    pub(super) fn validate_metadata(&self) -> Result<()> {
        for (name, ddl) in [(TABLE_META, TABLE_META_DDL), (COLUMN_META, COLUMN_META_DDL)] {
            if let Some(stored) = self.read_metadata_ddl(name)?
                && stored != ddl.trim()
            {
                return Err(SqliteStorageError::ConflictMetadataSchema(name.to_owned()));
            }
        }

        Ok(())
    }

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

    pub(super) fn require_schema(&self, table_name: &str) -> Result<Schema> {
        self.read_schema(table_name)?
            .ok_or_else(|| SqliteStorageError::TableNotFound(table_name.to_owned()))
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
}

fn parse_physical_schema(sql: &str) -> Result<Schema> {
    let sql = sql.strip_suffix(" STRICT").unwrap_or(sql);
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

struct ColumnMetadata {
    logical_type: String,
    comment: Option<String>,
    default_sql: Option<String>,
}

impl ColumnMetadata {
    fn restore_column(self, column: &mut ColumnDef) -> Result<()> {
        let data_type = parse_data_type(&self.logical_type)
            .and_then(|parsed| translate_data_type(&parsed))
            .map_err(|_| SqliteStorageError::InvalidLogicalType(self.logical_type.clone()))?;
        if data_type.to_string() != self.logical_type {
            return Err(SqliteStorageError::InvalidLogicalType(self.logical_type));
        }

        if physical_type(&data_type) != physical_type(&column.data_type) {
            return Err(SqliteStorageError::ConflictColumnType {
                expected: data_type,
                actual: column.data_type.to_string(),
            });
        }

        column.data_type = data_type;
        column.comment = self.comment;

        if let Some(sql) = self.default_sql {
            column.default = Some(translate_expr(&parse_expr(sql)?, &[])?);
        }

        Ok(())
    }
}

struct TableMetadata {
    is_schemaless: bool,
    comment: Option<String>,
}

impl TableMetadata {
    fn restore_table(self, schema: &mut Schema) -> Result<()> {
        schema.comment = self.comment;

        if !self.is_schemaless {
            return Ok(());
        }

        let valid = schema.column_defs.as_ref().is_some_and(|columns| {
            columns.len() == 1 && columns[0].name == "_doc" && columns[0].data_type == DataType::Map
        });
        if !valid {
            return Err(SqliteStorageError::ConflictSchemalessSchema(
                schema.table_name.clone(),
            ));
        }

        schema.column_defs = None;

        Ok(())
    }
}

impl SqliteStorage {
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
            self.connection.execute_batch(&create_table_sql(schema))?;
        }

        self.write_table_metadata(schema)?;
        self.write_column_metadata(&schema.table_name, &columns)
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

    fn write_column_metadata(&self, table_name: &str, columns: &[ColumnDef]) -> Result<()> {
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

    fn read_metadata_ddl(&self, name: &str) -> Result<Option<String>> {
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
            create_table_sql(new)
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

fn create_table_sql(schema: &Schema) -> String {
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

#[cfg(test)]
mod tests {
    use {
        super::{ColumnMetadata, TableMetadata},
        crate::SqliteStorageError,
        gluesql_core::{
            ast::{ColumnDef, DataType},
            data::Schema,
            error::{Error, TranslateError},
            parse_sql::parse_column_def,
            translate::translate_column_def,
        },
    };

    fn column(sql: &str) -> ColumnDef {
        let parsed = parse_column_def(sql).unwrap();

        translate_column_def(&parsed, &[]).unwrap()
    }

    #[test]
    fn parse_physical_schema() {
        for sql in ["", "SELECT 1"] {
            let actual = super::parse_physical_schema(sql);
            assert!(
                matches!(&actual, Err(SqliteStorageError::InvalidTableDefinition)),
                "{sql}: {actual:?}",
            );
        }

        // SQLite's REAL/BLOB and STRICT are translated before logical types are restored.
        let actual = super::parse_physical_schema(
            "CREATE TABLE item (
                 id INT NOT NULL PRIMARY KEY,
                 amount REAL DEFAULT 1.5,
                 bytes BLOB,
                 name TEXT UNIQUE
             ) STRICT",
        )
        .unwrap();
        let expected = Schema::from_ddl(
            "CREATE TABLE item (
                 id INT NOT NULL PRIMARY KEY,
                 amount FLOAT DEFAULT 1.5,
                 bytes BYTEA,
                 name TEXT UNIQUE
             )",
        )
        .unwrap();
        assert_eq!(actual, expected);
    }

    #[test]
    fn restore_column_type() {
        // Parsing, unsupported types, and aliases all reject the stored tag.
        for tag in ["", "NOT_A_TYPE", "INTEGER"] {
            let metadata = ColumnMetadata {
                logical_type: tag.to_owned(),
                comment: None,
                default_sql: None,
            };
            let actual = metadata.restore_column(&mut column("level INT"));
            assert!(
                matches!(
                    &actual,
                    Err(SqliteStorageError::InvalidLogicalType(actual)) if actual == tag
                ),
                "{tag}: {actual:?}",
            );
        }

        // A valid tag must still agree with the column's physical storage class.
        let metadata = ColumnMetadata {
            logical_type: "INT8".to_owned(),
            comment: None,
            default_sql: None,
        };
        let actual = metadata.restore_column(&mut column("level TEXT"));
        assert!(
            matches!(
                &actual,
                Err(SqliteStorageError::ConflictColumnType {
                    expected: DataType::Int8,
                    actual,
                }) if actual == "TEXT"
            ),
            "{actual:?}",
        );

        // Restoring INT8 keeps the name, constraints, and physical default.
        let metadata = ColumnMetadata {
            logical_type: "INT8".to_owned(),
            comment: None,
            default_sql: None,
        };
        let mut actual = column("level INT NOT NULL UNIQUE DEFAULT 1");
        metadata.restore_column(&mut actual).unwrap();
        let expected = column("level INT8 NOT NULL UNIQUE DEFAULT 1");
        assert_eq!(actual, expected);
    }

    #[test]
    fn restore_column_comment() {
        for (comment, expected) in [
            (None, "name TEXT"),
            (Some(""), "name TEXT COMMENT ''"),
            (Some("name"), "name TEXT COMMENT 'name'"),
        ] {
            let metadata = ColumnMetadata {
                logical_type: "TEXT".to_owned(),
                comment: comment.map(str::to_owned),
                default_sql: None,
            };
            let mut actual = column("name TEXT COMMENT 'old'");
            metadata.restore_column(&mut actual).unwrap();
            let expected = column(expected);
            assert_eq!(actual, expected);
        }
    }

    #[test]
    fn restore_column_default() {
        // Missing metadata preserves a default; SQL NULL explicitly replaces it.
        for (default_sql, expected) in [
            (None, "name TEXT DEFAULT 'original'"),
            (Some("LOWER('APPLE')"), "name TEXT DEFAULT LOWER('APPLE')"),
            (Some("NULL"), "name TEXT DEFAULT NULL"),
        ] {
            let metadata = ColumnMetadata {
                logical_type: "TEXT".to_owned(),
                comment: None,
                default_sql: default_sql.map(str::to_owned),
            };
            let mut actual = column("name TEXT DEFAULT 'original'");
            metadata.restore_column(&mut actual).unwrap();
            let expected = column(expected);
            assert_eq!(actual, expected);
        }

        // Preserve both parsing and translation failures from the stored expression.
        for (default_sql, expected) in [
            (
                "(",
                Error::Parser(
                    "ParserError(\n    \"Expected: an expression, found: EOF\",\n)".to_owned(),
                ),
            ),
            (
                "CAST(1 AS BADTYPE)",
                Error::Translate(TranslateError::UnsupportedDataType("BADTYPE".to_owned())),
            ),
        ] {
            let metadata = ColumnMetadata {
                logical_type: "TEXT".to_owned(),
                comment: None,
                default_sql: Some(default_sql.to_owned()),
            };
            let actual = metadata
                .restore_column(&mut column("name TEXT"))
                .map_err(Error::from);
            assert_eq!(actual, Err(expected), "{default_sql}");
        }
    }

    #[test]
    fn restore_table() {
        // An ordinary _doc MAP column stays schemaful without the document marker.
        for (comment, expected) in [
            (None, "CREATE TABLE item (_doc MAP)"),
            (Some(""), "CREATE TABLE item (_doc MAP) COMMENT = ''"),
            (
                Some("items"),
                "CREATE TABLE item (_doc MAP) COMMENT = 'items'",
            ),
        ] {
            let metadata = TableMetadata {
                is_schemaless: false,
                comment: comment.map(str::to_owned),
            };
            let mut actual =
                Schema::from_ddl("CREATE TABLE item (_doc MAP) COMMENT = 'old'").unwrap();
            metadata.restore_table(&mut actual).unwrap();
            let expected = Schema::from_ddl(expected).unwrap();
            assert_eq!(actual, expected);
        }

        // Document restoration needs columns, exactly one _doc, and its logical MAP type.
        for ddl in [
            "CREATE TABLE item",
            "CREATE TABLE item (_doc MAP, extra INT)",
            "CREATE TABLE item (document MAP)",
            "CREATE TABLE item (_doc TEXT)",
        ] {
            let metadata = TableMetadata {
                is_schemaless: true,
                comment: None,
            };
            let mut schema = Schema::from_ddl(ddl).unwrap();
            let actual = metadata.restore_table(&mut schema);
            assert!(
                matches!(
                    &actual,
                    Err(SqliteStorageError::ConflictSchemalessSchema(table)) if table == "item"
                ),
                "{ddl}: {actual:?}",
            );
        }

        let metadata = TableMetadata {
            is_schemaless: true,
            comment: Some("documents".to_owned()),
        };
        let mut actual = Schema::from_ddl("CREATE TABLE item (_doc MAP NOT NULL)").unwrap();
        metadata.restore_table(&mut actual).unwrap();
        let expected = Schema::from_ddl("CREATE TABLE item COMMENT = 'documents'").unwrap();
        assert_eq!(actual, expected);
    }

    #[test]
    fn create_table_sql() {
        // Integer primary keys use INT, while ordinary integer columns use INTEGER.
        let schema = Schema::from_ddl(
            "CREATE TABLE item (
                 id INT PRIMARY KEY,
                 level INT8 NOT NULL DEFAULT 1,
                 code INT UNIQUE,
                 label TEXT DEFAULT LOWER('APPLE') COMMENT 'label'
             ) COMMENT = 'items'",
        )
        .unwrap();
        let actual = super::create_table_sql(&schema);
        let expected = concat!(
            r#"CREATE TABLE "item" ("id" INT NOT NULL PRIMARY KEY, "#,
            r#""level" INTEGER NOT NULL DEFAULT 1, "code" INTEGER UNIQUE, "#,
            r#""label" TEXT) STRICT"#,
        );
        assert_eq!(actual, expected);

        // Foreign key definitions remain part of the physical table.
        let schema = Schema::from_ddl(
            "CREATE TABLE child (
                 parent_id INT,
                 CONSTRAINT parent_fk FOREIGN KEY (parent_id) REFERENCES parent (id)
                     ON DELETE NO ACTION ON UPDATE NO ACTION
             )",
        )
        .unwrap();
        let actual = super::create_table_sql(&schema);
        let expected = concat!(
            r#"CREATE TABLE "child" ("parent_id" INTEGER, "#,
            r#"CONSTRAINT "parent_fk" FOREIGN KEY ("parent_id") REFERENCES "parent" ("id") "#,
            "ON DELETE NO ACTION ON UPDATE NO ACTION) STRICT",
        );
        assert_eq!(actual, expected);

        let schema = Schema::from_ddl("CREATE TABLE documents").unwrap();
        let actual = super::create_table_sql(&schema);
        let expected = r#"CREATE TABLE "documents" ("_doc" TEXT NOT NULL) STRICT"#;
        assert_eq!(actual, expected);
    }
}
