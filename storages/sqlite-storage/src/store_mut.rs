use {
    super::SqliteStorage,
    crate::{
        operations::{
            insert_schemaless, insert_structured, insert_structured_with_key,
            update_schemaless_row, update_structured_row,
        },
        schema::{build_create_table_sql, primary_column},
        sql_builder::build_delete_sql,
    },
    async_trait::async_trait,
    gluesql_core::{
        data::{Key, Schema},
        error::{Error as GlueError, Result},
        store::{DataRow, StoreMut},
    },
    rusqlite::params_from_iter,
};

#[async_trait]
impl StoreMut for SqliteStorage {
    async fn insert_schema(&mut self, schema: &Schema) -> Result<()> {
        let exists = self.table_exists(&schema.table_name).await?;

        if !exists {
            let sql = build_create_table_sql(schema)?;
            self.execute(&sql).await?;
        }

        self.upsert_schema_meta(schema).await
    }

    async fn delete_schema(&mut self, table_name: &str) -> Result<()> {
        let sql = format!(
            r#"DROP TABLE IF EXISTS "{}""#,
            SqliteStorage::escape_identifier(table_name)
        );
        self.execute(&sql).await?;
        self.remove_schema_meta(table_name).await
    }

    async fn append_data(&mut self, table_name: &str, rows: Vec<DataRow>) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }

        let schema = self
            .ensure_schema(table_name)
            .await?
            .ok_or_else(|| GlueError::StorageMsg(format!("table '{table_name}' not found")))?;

        for row in rows {
            match row {
                DataRow::Vec(values) => {
                    if schema
                        .column_defs
                        .as_ref()
                        .is_some_and(|column_defs| column_defs.len() != values.len())
                    {
                        return Err(GlueError::StorageMsg(
                            "column count does not match values".to_owned(),
                        ));
                    }

                    insert_structured(self, table_name, &schema, values).await?;
                }
                DataRow::Map(map) => {
                    insert_schemaless(self, table_name, map).await?;
                }
            }
        }

        Ok(())
    }

    async fn insert_data(&mut self, table_name: &str, rows: Vec<(Key, DataRow)>) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }

        let schema = self
            .ensure_schema(table_name)
            .await?
            .ok_or_else(|| GlueError::StorageMsg(format!("table '{table_name}' not found")))?;

        let has_primary = primary_column(&schema).is_some();

        for (key, data_row) in rows {
            match data_row {
                DataRow::Vec(values) => {
                    if has_primary {
                        insert_structured_with_key(self, table_name, &schema, key, values).await?;
                    } else {
                        update_structured_row(self, table_name, &schema, key, values).await?;
                    }
                }
                DataRow::Map(map) => {
                    update_schemaless_row(self, table_name, key, map).await?;
                }
            }
        }

        Ok(())
    }

    async fn delete_data(&mut self, table_name: &str, keys: Vec<Key>) -> Result<()> {
        if keys.is_empty() {
            return Ok(());
        }

        let schema = self
            .ensure_schema(table_name)
            .await?
            .ok_or_else(|| GlueError::StorageMsg(format!("table '{table_name}' not found")))?;

        for key in keys {
            let (sql, params) = build_delete_sql(table_name, &schema, &key)?;
            self.with_conn(move |conn| {
                conn.execute(sql.as_str(), params_from_iter(params.iter()))
                    .map(|_| ())
            })
            .await?;
        }

        Ok(())
    }
}
