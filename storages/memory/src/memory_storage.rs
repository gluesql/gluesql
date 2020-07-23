use std::collections::HashMap;

use gluesql::{Result, Row, RowIter, Schema, Store, StoreError};

pub struct MemoryStorage {
    schema_map: HashMap<String, Schema>,
    data_map: HashMap<String, Vec<(u64, Row)>>,
    id: u64,
}

#[derive(Clone, Debug)]
pub struct DataKey {
    pub table_name: String,
    pub id: u64,
}

impl MemoryStorage {
    pub fn new() -> Result<Self> {
        let schema_map = HashMap::new();
        let data_map = HashMap::new();

        Ok(Self {
            schema_map,
            data_map,
            id: 1000,
        })
    }
}

impl Store<DataKey> for MemoryStorage {
    fn gen_id(&mut self, table_name: &str) -> Result<DataKey> {
        self.id += 1;

        let key = DataKey {
            table_name: table_name.to_string(),
            id: self.id,
        };

        Ok(key)
    }

    fn set_schema(&mut self, schema: &Schema) -> Result<()> {
        let table_name = schema.table_name.to_string();

        self.schema_map.insert(table_name, schema.clone());

        Ok(())
    }

    fn get_schema(&self, table_name: &str) -> Result<Schema> {
        let schema = self
            .schema_map
            .get(table_name)
            .ok_or(StoreError::SchemaNotFound)?
            .clone();

        Ok(schema)
    }

    fn del_schema(&mut self, table_name: &str) -> Result<()> {
        self.data_map.remove(table_name);
        self.schema_map.remove(table_name);

        Ok(())
    }

    fn set_data(&mut self, key: &DataKey, row: Row) -> Result<Row> {
        let DataKey { table_name, id } = key;
        let item = (*id, row.clone());

        match self.data_map.get_mut(table_name) {
            Some(items) => {
                match items.iter().position(|(item_id, _)| item_id == id) {
                    Some(index) => {
                        items[index] = item;
                    }
                    None => {
                        items.push(item);
                    }
                };
            }
            None => {
                self.data_map.insert(table_name.to_string(), vec![item]);
            }
        }

        Ok(row)
    }

    fn get_data(&self, table_name: &str) -> Result<RowIter<DataKey>> {
        let items = match self.data_map.get(table_name) {
            Some(items) => items
                .iter()
                .map(|(id, row)| {
                    let key = DataKey {
                        table_name: table_name.to_string(),
                        id: *id,
                    };

                    Ok((key, row.clone()))
                })
                .collect(),
            None => vec![],
        };

        Ok(Box::new(items.into_iter()))
    }

    fn del_data(&mut self, key: &DataKey) -> Result<()> {
        let DataKey { table_name, id } = key;

        let items = match self.data_map.get_mut(table_name) {
            Some(items) => items,
            None => {
                return Ok(());
            }
        };

        let index = match items.iter().position(|(item_id, _)| item_id == id) {
            Some(index) => index,
            None => {
                return Ok(());
            }
        };

        items.remove(index);

        Ok(())
    }
}
