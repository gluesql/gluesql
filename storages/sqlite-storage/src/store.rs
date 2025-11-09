use {
    super::SqliteStorage,
    crate::{
        codec::{build_key_params, decode_row_with_key},
        sql_builder::{build_select_all_sql, build_select_one_sql},
    },
    async_trait::async_trait,
    futures::stream,
    gluesql_core::{
        data::{Key, Schema},
        error::Result,
        store::{DataRow, RowIter, Store},
    },
    rusqlite::params_from_iter,
};

#[async_trait]
impl Store for SqliteStorage {
    async fn fetch_all_schemas(&self) -> Result<Vec<Schema>> {
        let rows = self
            .with_conn(|conn| {
                let mut stmt = conn.prepare(
                    "SELECT name, sql FROM sqlite_master WHERE type = 'table' ORDER BY name",
                )?;
                let entries = stmt
                    .query_map([], |row| {
                        let name: String = row.get(0)?;
                        let sql: Option<String> = row.get(1)?;
                        Ok((name, sql))
                    })?
                    .collect::<rusqlite::Result<Vec<_>>>()?;

                Ok(entries)
            })
            .await?;

        let mut schemas = Vec::with_capacity(rows.len());
        for (name, sql_opt) in rows {
            if name == "gluesql_schema" {
                continue;
            }

            if let Some(sql) = sql_opt.as_deref().and_then(SqliteStorage::decode_schema) {
                schemas.push(sql);
            } else if let Some(schema) = self.load_schema(&name).await? {
                schemas.push(schema);
            }
        }

        Ok(schemas)
    }

    async fn fetch_schema(&self, table_name: &str) -> Result<Option<Schema>> {
        self.ensure_schema(table_name).await
    }

    async fn fetch_data(&self, table_name: &str, key: &Key) -> Result<Option<DataRow>> {
        let Some(schema) = self.ensure_schema(table_name).await? else {
            return Ok(None);
        };

        let query = build_select_one_sql(table_name, &schema);
        let params = build_key_params(key, &schema)?;

        let schema_for_fetch = schema.clone();
        self.with_conn(move |conn| {
            let mut stmt = conn.prepare(query.as_str())?;
            let mut rows = stmt.query(params_from_iter(params.iter()))?;

            if let Some(row) = rows.next()? {
                let (_, data_row) =
                    decode_row_with_key(&schema_for_fetch, row).map_err(super::glue_to_rusqlite)?;
                Ok(Some(data_row))
            } else {
                Ok(None)
            }
        })
        .await
    }

    async fn scan_data<'a>(&'a self, table_name: &str) -> Result<RowIter<'a>> {
        let Some(schema) = self.ensure_schema(table_name).await? else {
            return Ok(Box::pin(stream::empty()));
        };

        let query = build_select_all_sql(table_name, &schema);
        let schema_clone = schema.clone();

        let rows = self
            .with_conn(move |conn| {
                let mut stmt = conn.prepare(query.as_str())?;
                let schema_inner = schema_clone.clone();
                let rows = stmt
                    .query_map([], move |row| {
                        decode_row_with_key(&schema_inner, row).map_err(super::glue_to_rusqlite)
                    })?
                    .collect::<rusqlite::Result<Vec<_>>>()?;
                Ok(rows)
            })
            .await?;

        let iter = rows.into_iter().map(|(key, row)| Ok((key, row)));
        Ok(Box::pin(stream::iter(iter)))
    }
}
