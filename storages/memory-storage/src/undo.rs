use crate::Log;
use crate::MemoryStorage;
use gluesql_core::ast::ColumnDef;
use gluesql_core::data::Value;
use gluesql_core::error::{AlterTableError, Error, Result};
use gluesql_core::store::DataRow;

impl MemoryStorage {
    pub async fn undo(&mut self, log: Log) -> Result<()> {
        match log {
            Log::InsertSchema(table_name) => {
                self.items.remove(&table_name);
                self.metadata.remove(&table_name);

                Ok(())
            }
            Log::RenameSchema(cur_table_name, prev_table_name) => {
                let mut item = self
                    .items
                    .remove(&cur_table_name)
                    .ok_or_else(|| AlterTableError::TableNotFound(cur_table_name.to_owned()))?;

                item.schema.table_name = prev_table_name.clone();
                self.items.insert(prev_table_name, item);

                Ok(())
            }
            Log::DeleteSchema(table_name, item, meta) => {
                self.metadata.insert(table_name.clone(), meta);
                self.items.insert(table_name, item);

                Ok(())
            }
            Log::RenameColumn(table_name, new_name, original_name) => {
                let item = self
                    .items
                    .get_mut(&table_name)
                    .ok_or_else(|| AlterTableError::TableNotFound(table_name.to_owned()))?;

                let column_defs =
                    item.schema.column_defs.as_mut().ok_or_else(|| {
                        AlterTableError::SchemalessTableFound(table_name.to_owned())
                    })?;

                let mut column_def = column_defs
                    .iter_mut()
                    .find(|column_def| column_def.name == new_name)
                    .ok_or(AlterTableError::RenamingColumnNotFound)?;

                column_def.name = original_name;

                Ok(())
            }
            Log::AddColumn(table_name, column_name) => {
                let item = self
                    .items
                    .get_mut(&table_name)
                    .ok_or_else(|| AlterTableError::TableNotFound(table_name.to_owned()))?;

                let column_defs =
                    item.schema.column_defs.as_mut().ok_or_else(|| {
                        AlterTableError::SchemalessTableFound(table_name.to_owned())
                    })?;

                let column_index = column_defs
                    .iter()
                    .position(|column_def| column_def.name == column_name)
                    .unwrap();

                column_defs.remove(column_index);

                for (_, row) in item.rows.iter_mut() {
                    if row.len() <= column_index {
                        continue;
                    }

                    match row {
                        DataRow::Vec(values) => {
                            values.remove(column_index);
                        }
                        _ => {
                            return Err(Error::StorageMsg(
                                "undo DROP COLUMN can't be done for schemaless".to_owned(),
                            ))
                        }
                    }
                }

                Ok(())
            }
            Log::DropColumn(table_name, column_def, column_index, pairs) => {
                let item = self.items.get_mut(&table_name).unwrap();
                let column_defs = item.schema.column_defs.as_mut().unwrap();
                column_defs.insert(column_index, column_def.clone());

                let ColumnDef {
                    data_type,
                    nullable,
                    default,
                    ..
                } = column_def;

                let value = match (default, nullable) {
                    (Some(expr), _) => {
                        let evaluated =
                            gluesql_core::executor::evaluate_stateless(None, &expr).await?;

                        evaluated.try_into_value(&data_type, nullable)?
                    }
                    (None, true) => Value::Null,
                    (None, false) => {
                        return Err(Error::StorageMsg(
                            "empty default field is impossible".to_owned(),
                        ));
                    }
                };

                for (key, row) in item.rows.iter_mut() {
                    match row {
                        DataRow::Vec(values) => {
                            if let Some((_, target_value)) = pairs.get_key_value(key) {
                                values.insert(column_index, target_value.to_owned());
                            } else {
                                values.push(value.clone())
                            }
                        }
                        _ => {
                            return Err(Error::StorageMsg(
                                "conflict - add_column failed: schemaless row found".to_owned(),
                            ))
                        }
                    }
                }

                Ok(())
            }
            Log::InsertData(table_name, keys) => {
                if let Some(item) = self.items.get_mut(&table_name) {
                    for key in keys {
                        item.rows.remove(&key);
                    }
                }
                Ok(())
            }
            Log::UpdateData(table_name, rows) => {
                if let Some(item) = self.items.get_mut(&table_name) {
                    for (key, row) in rows {
                        item.rows.insert(key, row);
                    }
                }
                Ok(())
            }
            Log::AppendData(table_name, keys) => {
                if let Some(item) = self.items.get_mut(&table_name) {
                    for key in keys {
                        item.rows.remove(&key);
                    }
                }
                Ok(())
            }
            Log::DeleteData(table_name, rows) => {
                if let Some(item) = self.items.get_mut(&table_name) {
                    for (key, row) in rows {
                        item.rows.insert(key, row);
                    }
                }
                Ok(())
            }
            Log::InsertFunction(func_name) => {
                self.functions.remove(&func_name);
                Ok(())
            }
            Log::DeleteFunction(func) => {
                self.functions.insert(func.func_name.to_uppercase(), func);
                Ok(())
            }
        }
    }
}
