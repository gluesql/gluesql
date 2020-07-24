use im_rc::{vector, HashMap, Vector};

use gluesql::{MutResult, MutStore, Result, Row, RowIter, Schema, Store, StoreError};

pub struct MemoryStorage {
    schema_map: HashMap<String, Schema>,
    data_map: HashMap<String, Vector<(u64, Row)>>,
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
            id: 0,
        })
    }
}

impl MutStore<DataKey> for MemoryStorage {
    fn gen_id(self, table_name: &str) -> MutResult<Self, DataKey> {
        let id = self.id + 1;
        let storage = Self {
            schema_map: self.schema_map,
            data_map: self.data_map,
            id,
        };

        let key = DataKey {
            table_name: table_name.to_string(),
            id,
        };

        Ok((storage, key))
    }

    fn set_schema(self, schema: &Schema) -> MutResult<Self, ()> {
        let table_name = schema.table_name.to_string();
        let schema_map = self.schema_map.update(table_name, schema.clone());
        let storage = Self {
            schema_map,
            data_map: self.data_map,
            id: self.id,
        };

        Ok((storage, ()))
    }

    fn del_schema(self, table_name: &str) -> MutResult<Self, ()> {
        let Self {
            mut schema_map,
            mut data_map,
            id,
        } = self;

        data_map.remove(table_name);
        schema_map.remove(table_name);
        let storage = Self {
            schema_map,
            data_map,
            id,
        };

        Ok((storage, ()))
    }

    fn set_data(self, key: &DataKey, row: Row) -> MutResult<Self, Row> {
        let DataKey { table_name, id } = key;
        let table_name = table_name.to_string();
        let item = (*id, row.clone());
        let Self {
            schema_map,
            data_map,
            id: self_id,
        } = self;

        let (mut items, data_map) = match data_map.extract(&table_name) {
            Some(v) => v,
            None => (vector![], data_map),
        };

        let items = match items.iter().position(|(item_id, _)| item_id == id) {
            Some(index) => items.update(index, item),
            None => {
                items.push_back(item);

                items
            }
        };

        let data_map = data_map.update(table_name, items);
        let storage = Self {
            schema_map,
            data_map,
            id: self_id,
        };

        Ok((storage, row))
    }

    fn del_data(self, key: &DataKey) -> MutResult<Self, ()> {
        let DataKey { table_name, id } = key;
        let table_name = table_name.to_string();
        let Self {
            schema_map,
            data_map,
            id: self_id,
        } = self;

        let (mut items, data_map) = match data_map.extract(&table_name) {
            Some(v) => v,
            None => (vector![], data_map),
        };

        if let Some(index) = items.iter().position(|(item_id, _)| item_id == id) {
            items.remove(index);
        };

        let data_map = data_map.update(table_name, items);
        let storage = Self {
            schema_map,
            data_map,
            id: self_id,
        };

        Ok((storage, ()))
    }
}

impl Store<DataKey> for MemoryStorage {
    fn get_schema(&self, table_name: &str) -> Result<Schema> {
        let schema = self
            .schema_map
            .get(table_name)
            .ok_or(StoreError::SchemaNotFound)?
            .clone();

        Ok(schema)
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

                    (key, row.clone())
                })
                .collect(),
            None => vector![],
        };

        let items = items.into_iter().map(Ok);

        Ok(Box::new(items))
    }
}
