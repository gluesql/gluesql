#![deny(clippy::str_to_string)]

mod alter_table;
mod index;
mod metadata;
mod transaction;

use {
    async_trait::async_trait,
    gluesql_core::{
        chrono::Utc,
        data::{CustomFunction as StructCustomFunction, Key, Schema, Value},
        error::{Error, Result},
        store::{CustomFunction, CustomFunctionMut, DataRow, RowIter, Store, StoreMut},
    },
    redis::{Commands, Connection},
    std::cell::RefCell,
    std::collections::{BTreeMap, HashMap},
};

pub struct Item {
    pub schema: Schema,
}

pub struct RedisStorage {
    pub namespace: String,
    pub items: HashMap<String, Item>,
    pub conn: RefCell<Connection>,
    pub metadata: HashMap<String, HashMap<String, Value>>,
    pub functions: HashMap<String, StructCustomFunction>,
}

impl RedisStorage {
    pub fn new(namespace: &str, url: &str, port: u16) -> Self {
        let redis_url = format!("redis://{}:{}", url, port);
        let mut conn = redis::Client::open(redis_url)
            .expect("Invalid connection URL")
            .get_connection()
            .expect("failed to connect to Redis");

        // TODO: read schemas from Redis if exist
        let mut items = HashMap::new();
        let scan_schema_key = format!("#schema#{}#*", namespace);
        let redis_keys: Vec<String> = conn
            .scan_match(&scan_schema_key)
            .map(|iter| iter.collect::<Vec<String>>())
            .map_err(|_| {
                Error::StorageMsg(format!(
                    "[RedisStorage] failed to scan schemas: namespace={}",
                    namespace
                ))
            })
            .unwrap(); // ignore error???

        // Then read all schemas of the namespace
        redis_keys.into_iter().for_each(|redis_key| {
            // Another client just has removed the value with the key.
            // It's not a problem. Just ignore it.
            if let Ok(value) = redis::cmd("GET").arg(&redis_key).query::<String>(&mut conn) {
                if let Ok(schema) = serde_json::from_str::<Schema>(&value) {
                    let table_name = schema.table_name.clone();
                    items.insert(table_name, Item { schema });
                }
            }
        });

        RedisStorage {
            namespace: namespace.to_owned(),
            items,
            conn: RefCell::new(conn),
            metadata: HashMap::new(),
            functions: HashMap::new(),
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

    fn redis_generate_schema_key(namespace: &str) -> String {
        format!("#schema#{}#", namespace)
    }

    fn redis_generate_metadata_key(
        namespace: &str,
        tablename: &str,
        metadata_name: &str,
    ) -> String {
        format!("#metadata#{}#{}#{}#", namespace, tablename, metadata_name)
    }

    fn redis_execute_get(&mut self, key: &str) -> Result<Option<String>> {
        let value = redis::cmd("GET")
            .arg(key)
            .query::<String>(&mut self.conn.get_mut())
            .map_err(|_| {
                Error::StorageMsg(format!("[RedisStorage] failed to execute GET: key={}", key))
            })?;

        Ok(Some(value))
    }

    fn redis_execute_set(&mut self, key: &str, value: &str) -> Result<()> {
        redis::cmd("SET")
            .arg(key)
            .arg(value)
            .query(&mut self.conn.get_mut())
            .map_err(|_| {
                Error::StorageMsg(format!(
                    "[RedisStorage] failed to execute SET: key={} value={}",
                    key, value
                ))
            })?;

        Ok(())
    }

    pub fn redis_execute_del(&mut self, key: &str) -> Result<()> {
        redis::cmd("DEL")
            .arg(key)
            .query(&mut self.conn.get_mut())
            .map_err(|_| {
                Error::StorageMsg(format!("[RedisStorage] failed to execute DEL: key={}", key))
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
            .map_err(|_| {
                Error::StorageMsg(format!("[RedisStorage] failed to scan data: key={}", key))
            })?;

        Ok(redis_keys)
    }
}

#[async_trait(?Send)]
impl CustomFunction for RedisStorage {
    async fn fetch_function(&self, func_name: &str) -> Result<Option<&StructCustomFunction>> {
        Ok(self.functions.get(&func_name.to_uppercase()))
    }
    async fn fetch_all_functions(&self) -> Result<Vec<&StructCustomFunction>> {
        Ok(self.functions.values().collect())
    }
}

#[async_trait(?Send)]
impl CustomFunctionMut for RedisStorage {
    async fn insert_function(&mut self, func: StructCustomFunction) -> Result<()> {
        self.functions.insert(func.func_name.to_uppercase(), func);
        Ok(())
    }

    async fn delete_function(&mut self, func_name: &str) -> Result<()> {
        self.functions.remove(&func_name.to_uppercase());
        Ok(())
    }
}

#[async_trait(?Send)]
impl Store for RedisStorage {
    async fn fetch_all_schemas(&self) -> Result<Vec<Schema>> {
        println!("fetch_all_schemas");
        let mut schemas = self
            .items
            .values()
            .map(|item| item.schema.clone())
            .collect::<Vec<_>>();
        schemas.sort_by(|a, b| a.table_name.cmp(&b.table_name));

        Ok(schemas)
    }
    async fn fetch_schema(&self, table_name: &str) -> Result<Option<Schema>> {
        println!("fetch_schema: table={}", table_name);
        self.items
            .get(table_name)
            .map(|item| Ok(item.schema.clone()))
            .transpose()
    }

    async fn fetch_data(&self, table_name: &str, key: &Key) -> Result<Option<DataRow>> {
        if self.items.get(table_name).is_some() {
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
        }
        Ok(None)
    }

    async fn scan_data(&self, table_name: &str) -> Result<RowIter> {
        let mut rows: BTreeMap<Key, DataRow> = BTreeMap::new();

        // First read all keys of the table
        let redis_keys: Vec<String> = self
            .conn
            .borrow_mut()
            .scan_match(&Self::redis_generate_scankey(&self.namespace, table_name))
            .map(|iter| iter.collect::<Vec<String>>())
            .map_err(|_| {
                Error::StorageMsg(format!(
                    "[RedisStorage] failed to scan data: namespace={} table_name={}",
                    self.namespace, table_name
                ))
            })?;

        // Then read all values of the table
        redis_keys.into_iter().for_each(|redis_key| {
            // Another client just has removed the value with the key.
            // It's not a problem. Just ignore it.
            if let Ok(value) = redis::cmd("GET")
                .arg(&redis_key)
                .query::<String>(&mut self.conn.borrow_mut())
            {
                if let Ok(key) = Self::redis_parse_key(&redis_key) {
                    if let Ok(row) = serde_json::from_str::<DataRow>(&value) {
                        rows.insert(key, row);
                    }
                }
            }
        });

        Ok(Box::new(rows.into_iter().map(Ok)))
    }
}

#[async_trait(?Send)]
impl StoreMut for RedisStorage {
    async fn insert_schema(&mut self, schema: &Schema) -> Result<()> {
        println!(
            "insert_schema: table={} schema={:?}",
            schema.table_name, schema
        );

        // TODO: store metadata into both of the DB and memory
        let current_time = Value::Timestamp(Utc::now().naive_utc());
        let created = HashMap::from([("CREATED".to_owned(), current_time.clone())]);
        let meta = HashMap::from([(schema.table_name.clone(), created)]);
        self.metadata.extend(meta);

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

        // store schema into both of the DB and memory
        let schema_value = serde_json::to_string(schema).map_err(|e| {
            Error::StorageMsg(format!(
                "[RedisStorage] failed to serialize schema={:?} error={}",
                schema, e
            ))
        })?;
        let schema_key = Self::redis_generate_schema_key(&self.namespace);
        self.redis_execute_set(&schema_key, &schema_value)?;

        let item = Item {
            schema: schema.clone(),
        };

        self.items.insert(table_name, item);

        Ok(())
    }

    async fn delete_schema(&mut self, table_name: &str) -> Result<()> {
        println!("delete_schema: table={}", table_name);

        if self.items.get(table_name).is_none() {
            // Ignore it if the table is already removed by another client
            // or the table is not found.
            return Ok(());
        }

        // delete rows
        let redis_key_iter: Vec<String> = self.redis_execute_scan(table_name)?;
        for key in redis_key_iter {
            self.redis_execute_del(&key)?;
        }

        // delete metadata
        self.metadata.remove(table_name);
        // TODO: delete metadata from the DB

        // delete schema
        self.items.remove(table_name);
        let schema_key = Self::redis_generate_schema_key(&self.namespace);
        self.redis_execute_del(&schema_key)?;

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
