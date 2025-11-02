use {
    crate::{SqliteStorage, codec::build_key_params, schema::primary_column},
    gluesql_core::{
        data::{Key, Schema},
        error::{Error as GlueError, Result},
    },
    rusqlite::types::Value as SqlValue,
};

pub(crate) fn build_select_one_sql(table_name: &str, schema: &Schema) -> String {
    match primary_column(schema) {
        Some(column) => format!(
            r#"SELECT {} FROM "{}" WHERE "{}" = ? LIMIT 1"#,
            select_projection(schema),
            SqliteStorage::escape_identifier(table_name),
            SqliteStorage::escape_identifier(column)
        ),
        None => format!(
            r#"SELECT rowid, {} FROM "{}" WHERE rowid = ? LIMIT 1"#,
            select_projection(schema),
            SqliteStorage::escape_identifier(table_name)
        ),
    }
}

pub(crate) fn build_select_all_sql(table_name: &str, schema: &Schema) -> String {
    match primary_column(schema) {
        Some(column) => format!(
            r#"SELECT {} FROM "{}" ORDER BY "{}""#,
            select_projection(schema),
            SqliteStorage::escape_identifier(table_name),
            SqliteStorage::escape_identifier(column)
        ),
        None => format!(
            r#"SELECT rowid, {} FROM "{}" ORDER BY rowid"#,
            select_projection(schema),
            SqliteStorage::escape_identifier(table_name)
        ),
    }
}

pub(crate) fn build_delete_sql(
    table_name: &str,
    schema: &Schema,
    key: &Key,
) -> Result<(String, Vec<SqlValue>)> {
    if let Some(column) = primary_column(schema) {
        return Ok((
            format!(
                r#"DELETE FROM "{}" WHERE "{}" = ?"#,
                SqliteStorage::escape_identifier(table_name),
                SqliteStorage::escape_identifier(column)
            ),
            build_key_params(key, schema)?,
        ));
    }

    let Key::I64(rowid) = key else {
        return Err(GlueError::StorageMsg("rowid must be an i64 key".to_owned()));
    };

    let rowid = *rowid;

    Ok((
        format!(
            r#"DELETE FROM "{}" WHERE rowid = ?"#,
            SqliteStorage::escape_identifier(table_name)
        ),
        vec![SqlValue::Integer(rowid)],
    ))
}

fn select_projection(schema: &Schema) -> String {
    match schema.column_defs.as_ref() {
        Some(columns) if !columns.is_empty() => columns
            .iter()
            .map(|col| format!(r#""{}""#, SqliteStorage::escape_identifier(&col.name)))
            .collect::<Vec<_>>()
            .join(", "),
        _ => "\"_gluesql_payload\"".to_owned(),
    }
}
