use {
    crate::{SqliteStorage, map_ser_err},
    base64::{Engine as _, engine::general_purpose::STANDARD as BASE64},
    gluesql_core::{
        ast::{ColumnDef, ColumnUniqueOption, DataType},
        data::Schema,
        error::Result,
    },
    rusqlite::{OptionalExtension, params},
};

const COMMENT_PREFIX: &str = "/*gluesql:";
const COMMENT_SUFFIX: &str = "*/";

impl SqliteStorage {
    pub(crate) fn decode_schema(sql: &str) -> Option<Schema> {
        let start = sql.find(COMMENT_PREFIX)?;
        let rest = &sql[start + COMMENT_PREFIX.len()..];
        let end = rest.find(COMMENT_SUFFIX)?;
        let encoded = &rest[..end];

        BASE64
            .decode(encoded)
            .ok()
            .and_then(|bytes| serde_json::from_slice::<Schema>(&bytes).ok())
    }

    pub(crate) fn encode_schema(schema: &Schema) -> Result<String> {
        let json = serde_json::to_string(schema).map_err(map_ser_err)?;
        Ok(format!(
            "{COMMENT_PREFIX}{encoded}{COMMENT_SUFFIX}",
            encoded = BASE64.encode(json)
        ))
    }

    pub(crate) async fn ensure_schema(&self, table_name: &str) -> Result<Option<Schema>> {
        if let Some(schema) = self.fetch_schema_from_meta(table_name).await? {
            return Ok(Some(schema));
        }

        let sql = format!(
            "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = '{}'",
            Self::escape_identifier(table_name)
        );

        if let Some(sql_text) = self
            .with_conn(move |conn| {
                conn.query_row(sql.as_str(), [], |row| row.get::<_, String>(0))
                    .optional()
            })
            .await?
            && let Some(schema) = Self::decode_schema(&sql_text)
        {
            self.upsert_schema_meta(&schema).await?;
            return Ok(Some(schema));
        }

        let schema = self.load_schema(table_name).await?;

        if let Some(schema) = &schema {
            self.upsert_schema_meta(schema).await?;
        }

        Ok(schema)
    }

    pub(crate) fn escape_identifier(value: &str) -> String {
        value.replace('"', "\"\"")
    }

    pub(crate) async fn load_schema(&self, table_name: &str) -> Result<Option<Schema>> {
        let pragma = format!(
            "PRAGMA table_info(\"{}\")",
            Self::escape_identifier(table_name)
        );
        let columns = self
            .with_conn(move |conn| {
                let mut stmt = conn.prepare(pragma.as_str())?;
                let entries = stmt
                    .query_map([], |row| {
                        let name: String = row.get("name")?;
                        let data_type: Option<String> = row.get("type")?;
                        let notnull: i64 = row.get("notnull")?;
                        let pk: i64 = row.get("pk")?;

                        Ok((name, data_type, notnull, pk))
                    })?
                    .collect::<rusqlite::Result<Vec<_>>>()?;

                Ok(entries)
            })
            .await?;

        if columns.is_empty() {
            return Ok(None);
        }

        let column_defs = columns
            .into_iter()
            .map(|(name, data_type, notnull, pk)| ColumnDef {
                name,
                data_type: data_type
                    .as_deref()
                    .and_then(sqlite_type_to_glue)
                    .unwrap_or(DataType::Text),
                nullable: notnull == 0,
                default: None,
                unique: (pk > 0).then_some(ColumnUniqueOption { is_primary: true }),
                comment: None,
            })
            .collect();

        Ok(Some(Schema {
            table_name: table_name.to_owned(),
            column_defs: Some(column_defs),
            indexes: vec![],
            engine: None,
            foreign_keys: vec![],
            comment: None,
        }))
    }

    async fn ensure_meta_table(&self) -> Result<()> {
        self.with_conn(|conn| {
            conn.execute(
                "CREATE TABLE IF NOT EXISTS gluesql_schema (
                    table_name TEXT PRIMARY KEY,
                    schema_json TEXT NOT NULL
                )",
                [],
            )
            .map(|_| ())
        })
        .await
    }

    pub(crate) async fn upsert_schema_meta(&self, schema: &Schema) -> Result<()> {
        self.ensure_meta_table().await?;

        let table_name = schema.table_name.clone();
        let json = serde_json::to_string(schema).map_err(map_ser_err)?;

        self.with_conn(move |conn| {
            conn.execute(
                "INSERT INTO gluesql_schema (table_name, schema_json) VALUES (?, ?)
                 ON CONFLICT(table_name) DO UPDATE SET schema_json = excluded.schema_json",
                params![table_name, json],
            )
            .map(|_| ())
        })
        .await
    }

    pub(crate) async fn remove_schema_meta(&self, table_name: &str) -> Result<()> {
        self.ensure_meta_table().await?;

        let table_name = table_name.to_owned();
        self.with_conn(move |conn| {
            conn.execute(
                "DELETE FROM gluesql_schema WHERE table_name = ?",
                params![table_name],
            )
            .map(|_| ())
        })
        .await
    }

    async fn fetch_schema_from_meta(&self, table_name: &str) -> Result<Option<Schema>> {
        self.ensure_meta_table().await?;

        let table_name = table_name.to_owned();
        let json = self
            .with_conn(move |conn| {
                conn.query_row(
                    "SELECT schema_json FROM gluesql_schema WHERE table_name = ?",
                    params![table_name],
                    |row| row.get::<_, String>(0),
                )
                .optional()
            })
            .await?;

        json.map(|json| serde_json::from_str(&json).map_err(map_ser_err))
            .transpose()
    }

    pub(crate) async fn table_exists(&self, table_name: &str) -> Result<bool> {
        let table_name = table_name.to_owned();
        self.with_conn(move |conn| {
            let mut stmt =
                conn.prepare("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?")?;
            let exists = stmt.exists(params![table_name])?;
            Ok(exists)
        })
        .await
    }
}

pub(crate) fn build_create_table_sql(schema: &Schema) -> Result<String> {
    let comment = SqliteStorage::encode_schema(schema)?;
    let columns = match schema.column_defs.as_ref() {
        Some(column_defs) if !column_defs.is_empty() => column_defs
            .iter()
            .map(sql_column_definition)
            .collect::<Vec<_>>()
            .join(", "),
        _ => "\"_gluesql_payload\" TEXT NOT NULL".to_owned(),
    };

    Ok(format!(
        r#"CREATE TABLE "{}" {} ({})"#,
        SqliteStorage::escape_identifier(&schema.table_name),
        comment,
        columns
    ))
}

pub(crate) fn primary_column(schema: &Schema) -> Option<&str> {
    schema.column_defs.as_ref().and_then(|columns| {
        columns.iter().find_map(|column| {
            (column.unique == Some(ColumnUniqueOption { is_primary: true }))
                .then_some(column.name.as_str())
        })
    })
}

fn sql_column_definition(column: &ColumnDef) -> String {
    let mut parts = vec![
        format!(r#""{}""#, SqliteStorage::escape_identifier(&column.name)),
        data_type_to_sqlite(&column.data_type),
    ];

    if !column.nullable {
        parts.push("NOT NULL".to_owned());
    }

    match column.unique {
        Some(ColumnUniqueOption { is_primary: true }) => parts.push("PRIMARY KEY".to_owned()),
        Some(ColumnUniqueOption { is_primary: false }) => parts.push("UNIQUE".to_owned()),
        None => {}
    }

    parts.join(" ")
}

fn data_type_to_sqlite(data_type: &DataType) -> String {
    match data_type {
        DataType::Boolean => "INTEGER".to_owned(),
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int | DataType::Time => {
            "INTEGER".to_owned()
        }
        DataType::Int128
        | DataType::Uint8
        | DataType::Uint16
        | DataType::Uint32
        | DataType::Uint64
        | DataType::Uint128
        | DataType::Uuid
        | DataType::Inet => "TEXT".to_owned(),
        DataType::Float32 | DataType::Float => "REAL".to_owned(),
        DataType::Text => "TEXT".to_owned(),
        DataType::Bytea => "BLOB".to_owned(),
        DataType::Date | DataType::Timestamp | DataType::Interval => "TEXT".to_owned(),
        DataType::Map | DataType::List | DataType::Point => "TEXT".to_owned(),
        DataType::Decimal => "TEXT".to_owned(),
    }
}

fn sqlite_type_to_glue(value: &str) -> Option<DataType> {
    let value = value.trim().to_uppercase();
    if value.contains("INT") {
        Some(DataType::Int)
    } else if value.contains("CHAR")
        || value.contains("CLOB")
        || value.contains("TEXT")
        || value.contains("STRING")
    {
        Some(DataType::Text)
    } else if value.contains("BLOB") {
        Some(DataType::Bytea)
    } else if value.contains("REAL") || value.contains("FLOA") || value.contains("DOUB") {
        Some(DataType::Float)
    } else if value.contains("NUMERIC") || value.contains("DECIMAL") {
        Some(DataType::Decimal)
    } else {
        None
    }
}
