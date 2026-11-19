use {
    crate::{Result, SqliteStorageError, codec::physical_type, collation::DECIMAL_COLLATION},
    gluesql_core::{
        ast::{ColumnDef, DataType, Expr, Literal, Statement, ToSql},
        data::{Schema, Value},
        parse_sql::{parse, parse_data_type, parse_expr},
        sqlparser::ast::{DataType as SqlDataType, Statement as SqlStatement},
        translate::{translate, translate_data_type, translate_expr},
    },
    std::borrow::Cow,
};

pub(super) const TABLE_META: &str = "__gluesql_table_meta";
pub(super) const COLUMN_META: &str = "__gluesql_column_meta";

pub(super) const TABLE_META_DDL: &str = "
CREATE TABLE __gluesql_table_meta (
    table_name TEXT COLLATE NOCASE NOT NULL PRIMARY KEY,
    is_schemaless INTEGER NOT NULL DEFAULT 0 CHECK (is_schemaless IN (0, 1)),
    comment TEXT
) STRICT
";

pub(super) const COLUMN_META_DDL: &str = "
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

pub(super) fn parse_physical_schema(sql: &str) -> Result<Schema> {
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

pub(super) struct ColumnMetadata {
    pub(super) logical_type: String,
    pub(super) comment: Option<String>,
    pub(super) default_sql: Option<String>,
}

impl ColumnMetadata {
    pub(super) fn restore_column(self, column: &mut ColumnDef) -> Result<()> {
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

pub(super) struct TableMetadata {
    pub(super) is_schemaless: bool,
    pub(super) comment: Option<String>,
}

impl TableMetadata {
    pub(super) fn restore_table(self, schema: &mut Schema) -> Result<()> {
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

pub(super) fn create_table_sql(schema: &Schema) -> String {
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
            if column.data_type == DataType::Decimal {
                sql.push_str(" COLLATE ");
                sql.push_str(DECIMAL_COLLATION);
            }

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

pub(super) fn qualified_column(table: &str, column: &str) -> String {
    format!("{}.{}", quote(table), quote(column))
}

pub(super) fn is_reserved(name: &str) -> bool {
    name.eq_ignore_ascii_case(TABLE_META) || name.eq_ignore_ascii_case(COLUMN_META)
}

pub(super) fn columns(schema: &Schema) -> Cow<'_, [ColumnDef]> {
    match &schema.column_defs {
        Some(columns) => Cow::Borrowed(columns),
        None => Cow::Owned(vec![document_column()]),
    }
}

pub(super) fn document_column() -> ColumnDef {
    ColumnDef {
        name: "_doc".to_owned(),
        data_type: DataType::Map,
        nullable: false,
        default: None,
        unique: None,
        comment: None,
    }
}

pub(super) fn primary_key(columns: &[ColumnDef]) -> Option<usize> {
    columns
        .iter()
        .position(|column| column.unique.is_some_and(|unique| unique.is_primary))
}

pub(super) fn key_column(schema: &Schema) -> Result<(&str, &DataType)> {
    if let Some(columns) = &schema.column_defs
        && let Some(index) = primary_key(columns)
    {
        let column = &columns[index];
        return Ok((&column.name, &column.data_type));
    }

    Ok((rowid_name(schema)?, &DataType::Int))
}

pub(super) fn key_type_supports_sqlite_order(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int
            | DataType::Uint8
            | DataType::Uint16
            | DataType::Uint32
            | DataType::Boolean
            | DataType::Text
            | DataType::Bytea
            | DataType::Decimal
    )
}

pub(super) fn rowid_name(schema: &Schema) -> Result<&'static str> {
    let columns = columns(schema);
    available_rowid(&columns, |column| &column.name)
        .ok_or_else(|| SqliteStorageError::RowidUnavailable(schema.table_name.clone()))
}

pub(super) fn available_rowid<T>(columns: &[T], name: impl Fn(&T) -> &str) -> Option<&'static str> {
    ["_rowid_", "rowid", "oid"].into_iter().find(|alias| {
        !columns
            .iter()
            .any(|column| name(column).eq_ignore_ascii_case(alias))
    })
}

pub(super) fn physical_default(expr: &Expr) -> bool {
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
                 name TEXT UNIQUE,
                 price TEXT COLLATE gluesql_decimal
             ) STRICT",
        )
        .unwrap();
        let expected = Schema::from_ddl(
            "CREATE TABLE item (
                 id INT NOT NULL PRIMARY KEY,
                 amount FLOAT DEFAULT 1.5,
                 bytes BYTEA,
                 name TEXT UNIQUE,
                 price TEXT
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

        // Decimal comparisons use the same numeric collation for keys and ordinary columns.
        let schema = Schema::from_ddl(
            "CREATE TABLE prices (
                 id DECIMAL PRIMARY KEY,
                 code DECIMAL UNIQUE,
                 amount DECIMAL
             )",
        )
        .unwrap();
        let actual = super::create_table_sql(&schema);
        let expected = concat!(
            r#"CREATE TABLE "prices" ("id" TEXT COLLATE gluesql_decimal NOT NULL PRIMARY KEY, "#,
            r#""code" TEXT COLLATE gluesql_decimal UNIQUE, "#,
            r#""amount" TEXT COLLATE gluesql_decimal) STRICT"#,
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
