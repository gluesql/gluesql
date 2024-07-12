use {
    super::RedisStorage,
    async_trait::async_trait,
    gluesql_core::{
        ast::ColumnDef,
        data::Value,
        error::{AlterTableError, Error, Result},
        store::{AlterTable, DataRow, Store},
    },
    redis::Commands,
};

#[async_trait(?Send)]
impl AlterTable for RedisStorage {
    async fn rename_schema(&mut self, table_name: &str, new_table_name: &str) -> Result<()> {
        if let Some(mut schema) = self.fetch_schema(table_name).await? {
            // Which should be done first? deleting or storing?
            self.redis_delete_schema(table_name)?;

            new_table_name.clone_into(&mut schema.table_name);
            self.redis_store_schema(&schema)?;

            let redis_key_iter: Vec<String> = self.redis_execute_scan(table_name)?;

            for redis_key in redis_key_iter {
                if let Some(value) = self.redis_execute_get(&redis_key)? {
                    let key = Self::redis_parse_key(&redis_key)?;
                    let new_key = Self::redis_generate_key(&self.namespace, new_table_name, &key)?;

                    self.redis_execute_set(&new_key, &value)?;
                    self.redis_execute_del(&redis_key)?;
                }
            }
        } else {
            return Err(AlterTableError::TableNotFound(table_name.to_owned()).into());
        }

        Ok(())
    }

    async fn rename_column(
        &mut self,
        table_name: &str,
        old_column_name: &str,
        new_column_name: &str,
    ) -> Result<()> {
        if let Some(mut schema) = self.fetch_schema(table_name).await? {
            let column_defs = schema
                .column_defs
                .as_mut()
                .ok_or_else(|| AlterTableError::SchemalessTableFound(table_name.to_owned()))?;

            if column_defs
                .iter()
                .any(|ColumnDef { name, .. }| name == new_column_name)
            {
                return Err(
                    AlterTableError::AlreadyExistingColumn(new_column_name.to_owned()).into(),
                );
            }

            let column_def = column_defs
                .iter_mut()
                .find(|column_def| column_def.name == old_column_name)
                .ok_or(AlterTableError::RenamingColumnNotFound)?;

            new_column_name.clone_into(&mut column_def.name);

            self.redis_delete_schema(table_name)?;
            self.redis_store_schema(&schema)?;
        } else {
            return Err(AlterTableError::TableNotFound(table_name.to_owned()).into());
        }

        Ok(())
    }

    async fn add_column(&mut self, table_name: &str, column_def: &ColumnDef) -> Result<()> {
        if let Some(mut schema) = self.fetch_schema(table_name).await? {
            let column_defs = schema
                .column_defs
                .as_mut()
                .ok_or_else(|| AlterTableError::SchemalessTableFound(table_name.to_owned()))?;

            if column_defs
                .iter()
                .any(|ColumnDef { name, .. }| name == &column_def.name)
            {
                let adding_column = column_def.name.to_owned();

                return Err(AlterTableError::AlreadyExistingColumn(adding_column).into());
            }

            let ColumnDef {
                data_type,
                nullable,
                default,
                ..
            } = column_def;

            let new_value_of_new_column = match (default, nullable) {
                (Some(expr), _) => {
                    let evaluated = gluesql_core::executor::evaluate_stateless(None, expr).await?;

                    evaluated.try_into_value(data_type, *nullable)?
                }
                (None, true) => Value::Null,
                (None, false) => {
                    return Err(AlterTableError::DefaultValueRequired(column_def.clone()).into())
                }
            };

            // NOTE: It cannot call self.redis_execute_scan/get/set methods directly.
            // column_defs has a reference to item and the item has a reference to self.
            // Therefore it cannot call self.redis_execute_scan method because
            // it needs to use the mutable reference of self.
            // Otherwise, it will cause a mutable reference conflict.
            let scan_key = Self::redis_generate_scankey(&self.namespace, table_name);
            let key_iter: Vec<String> = self
                .conn
                .borrow_mut()
                .scan_match(&scan_key)
                .map(|iter| iter.collect::<Vec<String>>())
                .map_err(|_| {
                    Error::StorageMsg(format!(
                        "[RedisStorage] failed to execute SCAN: key={}",
                        scan_key
                    ))
                })?;

            for key in key_iter {
                let value = redis::cmd("GET")
                    .arg(&key)
                    .query::<String>(&mut self.conn.borrow_mut())
                    .map_err(|_| {
                        Error::StorageMsg(format!(
                            "[RedisStorage] failed to execute GET: key={}",
                            key
                        ))
                    })?;

                let mut row: DataRow = serde_json::from_str(&value).map_err(|e| {
                    Error::StorageMsg(format!(
                        "[RedisStorage] failed to deserialize value={} error={}",
                        value, e
                    ))
                })?;
                match &mut row {
                    DataRow::Vec(values) => {
                        values.push(new_value_of_new_column.clone());
                    }
                    DataRow::Map(_) => {
                        return Err(Error::StorageMsg(
                            "[RedisStorage] conflict - add_column failed: schemaless row found"
                                .to_owned(),
                        ));
                    }
                }

                let new_value = serde_json::to_string(&row).map_err(|_e| {
                    Error::StorageMsg(format!(
                        "[RedisStorage] failed to serialize row={:?} error={}",
                        row, _e
                    ))
                })?;
                let _: () = redis::cmd("SET")
                    .arg(&key)
                    .arg(new_value)
                    .query(&mut self.conn.borrow_mut())
                    .map_err(|_| {
                        Error::StorageMsg(format!(
                            "[RedisStorage] add_column: failed to execute SET for row={:?}",
                            row
                        ))
                    })?;
            }

            column_defs.push(column_def.clone());
            self.redis_delete_schema(table_name)?; // No problem yet, finally it's ok to delete the old schema
            self.redis_store_schema(&schema)?;
        } else {
            return Err(AlterTableError::TableNotFound(table_name.to_owned()).into());
        }

        Ok(())
    }

    async fn drop_column(
        &mut self,
        table_name: &str,
        column_name: &str,
        if_exists: bool,
    ) -> Result<()> {
        if let Some(mut schema) = self.fetch_schema(table_name).await? {
            let column_defs = schema
                .column_defs
                .as_mut()
                .ok_or_else(|| AlterTableError::SchemalessTableFound(table_name.to_owned()))?;

            let column_index = column_defs
                .iter()
                .position(|column_def| column_def.name == column_name);

            match column_index {
                Some(column_index) => {
                    column_defs.remove(column_index);

                    let key_iter = self.redis_execute_scan(table_name)?;
                    for key in key_iter {
                        if let Some(value) = self.redis_execute_get(&key)? {
                            let mut row: DataRow = serde_json::from_str(&value).map_err(|e| {
                                Error::StorageMsg(format!(
                                    "[RedisStorage] failed to deserialize value={} error={}",
                                    value, e
                                ))
                            })?;
                            match &mut row {
                                DataRow::Vec(values) => {
                                    values.remove(column_index);
                                }
                                DataRow::Map(_) => {
                                    return Err(Error::StorageMsg(
                                    "[RedisStorage] conflict - add_column failed: schemaless row found".to_owned(),
                                ));
                                }
                            }

                            let new_value = serde_json::to_string(&row).map_err(|e| {
                                Error::StorageMsg(format!(
                                    "[RedisStorage] failed to serialize row={:?} error={}",
                                    row, e
                                ))
                            })?;
                            self.redis_execute_set(&key, &new_value)?;
                        }
                    }
                }
                None if if_exists => {}
                None => {
                    return Err(
                        AlterTableError::DroppingColumnNotFound(column_name.to_owned()).into(),
                    )
                }
            };

            self.redis_delete_schema(table_name)?; // No problem yet, finally it's ok to delete the old schema
            self.redis_store_schema(&schema)?;
        } else {
            return Err(AlterTableError::TableNotFound(table_name.to_owned()).into());
        }

        Ok(())
    }
}
