#![deny(clippy::str_to_string)]

mod alter_table;
mod index;
mod metadata;
mod transaction;

use {
    async_trait::async_trait,
    futures::stream::iter,
    gluesql_core::{
        chrono::Utc,
        data::{CustomFunction as StructCustomFunction, Key, Schema, Value},
        error::{Error, Result},
        store::{CustomFunction, CustomFunctionMut, DataRow, RowIter, Store, StoreMut},
    },
    redis::{Commands, Connection},
    std::{cell::RefCell, collections::BTreeMap},
};

pub struct RedisStorage {
    pub namespace: String,
    pub conn: RefCell<Connection>,
}

impl RedisStorage {
    pub fn new(namespace: &str, url: &str, port: u16) -> Self {
        let redis_url = format!("redis://{}:{}", url, port);
        let conn = redis::Client::open(redis_url)
            .expect("Invalid connection URL")
            .get_connection()
            .expect("failed to connect to Redis");

        RedisStorage {
            namespace: namespace.to_owned(),
            conn: RefCell::new(conn),
        }
    }

    ///
    /// Make a key to insert/delete a value with the namespace, table-name.
    ///
    /// Redis documentation recommends to use ':' as a separator for namespace and table-name.
    /// But it is not a good idea when using serde_json to serialize/deserialize a key.
    /// JSON uses ':' as a separator for key and value. So it conflicts with the JSON format.
    /// Therefore I use '#' as a separator: "namespace"#"table-name"#"key"#"value".
    ///
    fn redis_generate_key(namespace: &str, table_name: &str, key: &Key) -> Result<String> {
        let k = serde_json::to_string(key).map_err(|e| {
            Error::StorageMsg(format!(
                "[RedisStorage] failed to serialize key key:{:?}, error={}",
                key, e
            ))
        })?;
        Ok(format!("{}#{}#{}", namespace, table_name, k))
    }

    ///
    /// Parse a redis-key to get the original key of the table
    ///
    pub fn redis_parse_key(redis_key: &str) -> Result<Key> {
        let split_key = redis_key.split('#').collect::<Vec<&str>>();
        serde_json::from_str(split_key[2]).map_err(|e| {
            Error::StorageMsg(format!(
                "[RedisStorage] failed to deserialize key: key={} error={}",
                redis_key, e
            ))
        })
    }

    ///
    /// Make a key pattern to do scan and get all data in the namespace
    ///
    fn redis_generate_scankey(namespace: &str, tablename: &str) -> String {
        // First I used "{}#{}*" pattern. It had a problem when using
        // similar table-names such like Test, TestA and TestB.
        // When scanning Test, it gets all data from Test, TestA and TestB.
        // Therefore it is very important to use the # twice.
        format!("{}#{}#*", namespace, tablename)
    }

    ///
    /// Make a key pattern to do scan and get all schemas in the namespace
    ///
    fn redis_generate_schema_key(namespace: &str, table_name: &str) -> String {
        format!("#schema#{}#{}#", namespace, table_name)
    }

    fn redis_generate_scan_schema_key(namespace: &str) -> String {
        format!("#schema#{}#*", namespace)
    }

    fn redis_generate_metadata_key(
        namespace: &str,
        tablename: &str,
        metadata_name: &str,
    ) -> String {
        format!("#metadata#{}#{}#{}#", namespace, tablename, metadata_name)
    }

    fn redis_generate_scan_metadata_key(namespace: &str, tablename: &str) -> String {
        format!("#metadata#{}#{}#*", namespace, tablename)
    }

    fn redis_generate_scan_all_metadata_key(namespace: &str) -> String {
        format!("#metadata#{}#*", namespace)
    }

    fn redis_execute_get(&mut self, key: &str) -> Result<Option<String>> {
        let value = redis::cmd("GET")
            .arg(key)
            .query::<String>(&mut self.conn.get_mut())
            .map_err(|e| {
                Error::StorageMsg(format!(
                    "[RedisStorage] failed to execute GET: key={} error={}",
                    key, e
                ))
            })?;

        Ok(Some(value))
    }

    fn redis_execute_set(&mut self, key: &str, value: &str) -> Result<()> {
        redis::cmd("SET")
            .arg(key)
            .arg(value)
            .query(&mut self.conn.get_mut())
            .map_err(|e| {
                Error::StorageMsg(format!(
                    "[RedisStorage] failed to execute SET: key={} value={} error={}",
                    key, value, e
                ))
            })?;

        Ok(())
    }

    pub fn redis_execute_del(&mut self, key: &str) -> Result<()> {
        redis::cmd("DEL")
            .arg(key)
            .query(&mut self.conn.get_mut())
            .map_err(|e| {
                Error::StorageMsg(format!(
                    "[RedisStorage] failed to execute DEL: key={} error={}",
                    key, e
                ))
            })?;

        Ok(())
    }

    pub fn redis_execute_scan(&mut self, table_name: &str) -> Result<Vec<String>> {
        let key = Self::redis_generate_scankey(&self.namespace, table_name);
        let redis_keys: Vec<String> = self
            .conn
            .get_mut()
            .scan_match(&key)
            .map(|iter| iter.collect::<Vec<String>>())
            .map_err(|e| {
                Error::StorageMsg(format!(
                    "[RedisStorage] failed to scan data: key={} error={}",
                    key, e
                ))
            })?;

        Ok(redis_keys)
    }

    pub fn redis_store_schema(&mut self, schema: &Schema) -> Result<()> {
        let schema_value = serde_json::to_string(schema).map_err(|e| {
            Error::StorageMsg(format!(
                "[RedisStorage] failed to serialize schema={:?} error={}",
                schema, e
            ))
        })?;
        let schema_key = Self::redis_generate_schema_key(&self.namespace, &schema.table_name);
        self.redis_execute_set(&schema_key, &schema_value)?;

        Ok(())
    }

    pub fn redis_delete_schema(&mut self, table_name: &str) -> Result<()> {
        let schema_key = Self::redis_generate_schema_key(&self.namespace, table_name);
        // It's already if the schema is already removed by another client.
        if let Ok(Some(schema_value)) = self.redis_execute_get(&schema_key) {
            let schema = serde_json::from_str::<Schema>(&schema_value).map_err(|e| {
                Error::StorageMsg(format!(
                    "[RedisStorage] failed to deserialize schema={:?} error={}",
                    schema_value, e
                ))
            })?;
            if schema.table_name == table_name {
                self.redis_execute_del(&schema_key)?;
            }
        }

        Ok(())
    }
}

#[async_trait(?Send)]
impl CustomFunction for RedisStorage {
    async fn fetch_function(&self, _func_name: &str) -> Result<Option<&StructCustomFunction>> {
        Err(Error::StorageMsg(
            "[RedisStorage] fetch_function is not supported yet".to_owned(),
        ))
    }

    async fn fetch_all_functions(&self) -> Result<Vec<&StructCustomFunction>> {
        Err(Error::StorageMsg(
            "[RedisStorage] fetch_all_functions is not supported yet".to_owned(),
        ))
    }
}

#[async_trait(?Send)]
impl CustomFunctionMut for RedisStorage {
    async fn insert_function(&mut self, _func: StructCustomFunction) -> Result<()> {
        Err(Error::StorageMsg(
            "[RedisStorage] insert_function is not supported yet".to_owned(),
        ))
    }

    async fn delete_function(&mut self, _func_name: &str) -> Result<()> {
        Err(Error::StorageMsg(
            "[RedisStorage] delete_function is not supported yet".to_owned(),
        ))
    }
}

#[async_trait(?Send)]
impl Store for RedisStorage {
    async fn fetch_all_schemas(&self) -> Result<Vec<Schema>> {
        let mut schemas = Vec::<Schema>::new();
        let scan_schema_key = Self::redis_generate_scan_schema_key(&self.namespace);
        let redis_keys: Vec<String> = self
            .conn
            .borrow_mut()
            .scan_match(&scan_schema_key)
            .map(|iter| iter.collect::<Vec<String>>())
            .map_err(|e| {
                Error::StorageMsg(format!(
                    "[RedisStorage] failed to scan schemas: namespace={} error={}",
                    self.namespace, e
                ))
            })?;

        // Then read all schemas of the namespace
        for redis_key in redis_keys.into_iter() {
            // Another client just has removed the value with the key.
            // It's not a problem. Just ignore it.
            if let Ok(value) = redis::cmd("GET")
                .arg(&redis_key)
                .query::<String>(&mut self.conn.borrow_mut())
            {
                serde_json::from_str::<Schema>(&value)
                    .map_err(|e| {
                        Error::StorageMsg(format!(
                            "[RedisStorage] failed to deserialize schema={} error={}",
                            value, e
                        ))
                    })
                    .map(|schema| schemas.push(schema))?;
            }
        }

        schemas.sort_by(|a, b| a.table_name.cmp(&b.table_name));

        Ok(schemas)
    }

    async fn fetch_schema(&self, table_name: &str) -> Result<Option<Schema>> {
        let mut found = None;
        let scan_schema_key = Self::redis_generate_scan_schema_key(&self.namespace);
        let redis_keys: Vec<String> = self
            .conn
            .borrow_mut()
            .scan_match(&scan_schema_key)
            .map(|iter| iter.collect::<Vec<String>>())
            .map_err(|e| {
                Error::StorageMsg(format!(
                    "[RedisStorage] failed to scan schemas: namespace={} error={}",
                    self.namespace, e
                ))
            })?;

        // Then read all schemas of the namespace
        for redis_key in redis_keys.into_iter() {
            // Another client just has removed the value with the key.
            // It's not a problem. Just ignore it.
            if let Ok(value) = redis::cmd("GET")
                .arg(&redis_key)
                .query::<String>(&mut self.conn.borrow_mut())
            {
                serde_json::from_str::<Schema>(&value)
                    .map_err(|e| {
                        Error::StorageMsg(format!(
                            "[RedisStorage] failed to deserialize schema={} error={}",
                            value, e
                        ))
                    })
                    .map(|schema| {
                        if schema.table_name == table_name {
                            found = Some(schema);
                        }
                    })?;
            }

            if found.is_some() {
                break;
            }
        }

        Ok(found)
    }

    async fn fetch_data(&self, table_name: &str, key: &Key) -> Result<Option<DataRow>> {
        let key = Self::redis_generate_key(&self.namespace, table_name, key)?;
        // It's not a problem if the value with the key is removed by another client.
        if let Ok(value) = redis::cmd("GET")
            .arg(&key)
            .query::<String>(&mut self.conn.borrow_mut())
        {
            return serde_json::from_str::<DataRow>(&value)
                .map_err(|e| {
                    Error::StorageMsg(format!(
                        "[RedisStorage] failed to deserialize value={} error={:?}",
                        value, e
                    ))
                })
                .map(Some);
        }
        Ok(None)
    }

    async fn scan_data(&self, table_name: &str) -> Result<RowIter> {
        // First read all keys of the table
        let redis_keys: Vec<String> = self
            .conn
            .borrow_mut()
            .scan_match(&Self::redis_generate_scankey(&self.namespace, table_name))
            .map(|iter| iter.collect::<Vec<String>>())
            .map_err(|e| {
                Error::StorageMsg(format!(
                    "[RedisStorage] failed to scan data: namespace={} table_name={} error={}",
                    self.namespace, table_name, e
                ))
            })?;

        let rows = redis_keys
            .into_iter()
            .filter_map(|redis_key| {
                // Another client just has removed the value with the key.
                // It's not a problem. Just ignore it.
                redis::cmd("GET")
                    .arg(&redis_key)
                    .query::<String>(&mut self.conn.borrow_mut())
                    .ok()
                    .map(|value| (redis_key, value))
            })
            .map(|(redis_key, value)| {
                let key = Self::redis_parse_key(&redis_key).map_err(|e| {
                    Error::StorageMsg(format!(
                        "[RedisStorage] Wrong key format: key={} error={}",
                        redis_key, e
                    ))
                })?;

                let row = serde_json::from_str::<DataRow>(&value).map_err(|e| {
                    Error::StorageMsg(format!(
                        "[RedisStorage] failed to deserialize value={} error={:?}",
                        value, e
                    ))
                })?;
                Ok((key, row))
            })
            .collect::<Result<BTreeMap<Key, DataRow>>>()?;
        Ok(Box::pin(iter(rows.into_iter().map(Ok))))
    }
}

#[async_trait(?Send)]
impl StoreMut for RedisStorage {
    async fn insert_schema(&mut self, schema: &Schema) -> Result<()> {
        let current_time = Value::Timestamp(Utc::now().naive_utc());
        let current_time_value = serde_json::to_string(&current_time).map_err(|e| {
            Error::StorageMsg(format!(
                "[RedisStorage] failed to serialize metadata={:?} error={}",
                current_time, e
            ))
        })?;
        let metadata_key =
            Self::redis_generate_metadata_key(&self.namespace, &schema.table_name, "CREATED");
        self.redis_execute_set(&metadata_key, &current_time_value)?;

        let table_name = schema.table_name.clone();
        let metadata_key =
            Self::redis_generate_metadata_key(&self.namespace, &table_name, "CREATED");
        let metadata_value = serde_json::to_string(&current_time).map_err(|e| {
            Error::StorageMsg(format!(
                "[RedisStorage] failed to serialize metadata={:?} error={}",
                current_time, e
            ))
        })?;
        self.redis_execute_set(&metadata_key, &metadata_value)?;

        // Finally it's ok to store the new schema
        self.redis_store_schema(schema)?;

        Ok(())
    }

    async fn delete_schema(&mut self, table_name: &str) -> Result<()> {
        let redis_key_iter: Vec<String> = self.redis_execute_scan(table_name)?;
        for key in redis_key_iter {
            self.redis_execute_del(&key)?;
        }

        // delete metadata
        let metadata_scan_key = Self::redis_generate_scan_metadata_key(&self.namespace, table_name);
        let metadata_redis_keys: Vec<String> = self
            .conn
            .borrow_mut()
            .scan_match(&metadata_scan_key)
            .map(|iter| iter.collect::<Vec<String>>())
            .map_err(|e| {
                Error::StorageMsg(format!(
                    "[RedisStorage] failed to scan metadata: namespace={} table_name={} error={}",
                    self.namespace, table_name, e
                ))
            })?;
        for key in metadata_redis_keys {
            self.redis_execute_del(&key)?;
        }

        self.redis_delete_schema(table_name)?;

        Ok(())
    }

    async fn append_data(&mut self, table_name: &str, rows: Vec<DataRow>) -> Result<()> {
        for row in rows {
            // Even multiple clients can get an unique value with INCR command.
            // and a shared key "globalkey"
            let k = redis::cmd("INCR")
                .arg("globalkey")
                .query::<i64>(&mut self.conn.borrow_mut())
                .map_err(|_| {
                    Error::StorageMsg("[RedisStorage] failed to execute INCR".to_owned())
                })?;
            let key = Key::I64(k);
            let redis_key = Self::redis_generate_key(&self.namespace, table_name, &key)?;
            let value = serde_json::to_string(&row).map_err(|e| {
                Error::StorageMsg(format!(
                    "[RedisStorage] failed to serialize row={:?} error={}",
                    row, e
                ))
            })?;

            self.redis_execute_set(&redis_key, &value)?;
        }

        Ok(())
    }

    async fn insert_data(&mut self, table_name: &str, rows: Vec<(Key, DataRow)>) -> Result<()> {
        for (key, row) in rows {
            let redis_key = Self::redis_generate_key(&self.namespace, table_name, &key)?;
            let value = serde_json::to_string(&row).map_err(|e| {
                Error::StorageMsg(format!(
                    "[RedisStorage] failed to serialize row={:?} error={}",
                    row, e
                ))
            })?;
            self.redis_execute_set(&redis_key, &value)?;
        }

        Ok(())
    }

    async fn delete_data(&mut self, table_name: &str, keys: Vec<Key>) -> Result<()> {
        for key in keys {
            let redis_key = Self::redis_generate_key(&self.namespace, table_name, &key)?;
            self.redis_execute_del(&redis_key)?;
        }

        Ok(())
    }
}
