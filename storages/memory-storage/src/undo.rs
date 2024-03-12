use crate::Log;
use crate::MemoryStorage;
use gluesql_core::error::Result;
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
                let mut item = self.items.remove(&cur_table_name).unwrap();
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
                let item = self.items.get_mut(&table_name).unwrap();
                let column_defs = item.schema.column_defs.as_mut().unwrap();
                let column_def = column_defs
                    .iter_mut()
                    .find(|column_def| column_def.name == new_name)
                    .unwrap();

                column_def.name = original_name;

                Ok(())
            }
            Log::AddColumn(table_name, column_name) => {
                let item = self.items.get_mut(&table_name).unwrap();

                let column_defs = item.schema.column_defs.as_mut().unwrap();

                let column_index = column_defs
                    .iter()
                    .position(|column_def| column_def.name == column_name)
                    .unwrap();

                column_defs.remove(column_index);

                for (_, row) in item.rows.iter_mut() {
                    if let DataRow::Vec(values) = row {
                        values.remove(column_index);
                    }
                }

                Ok(())
            }
            Log::DropColumn(table_name, column_def, column_index, pairs) => {
                let item = self.items.get_mut(&table_name).unwrap();
                let column_defs = item.schema.column_defs.as_mut().unwrap();
                column_defs.insert(column_index, column_def);

                for (key, row) in item.rows.iter_mut() {
                    if let DataRow::Vec(values) = row {
                        if let Some((_, target_value)) = pairs.get_key_value(key) {
                            values.insert(column_index, target_value.to_owned());
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
        }
    }
}
