use {
    crate::RedisStorage,
    async_trait::async_trait,
    gluesql_core::{
        data::Value,
        error::{Error, Result},
        store::{MetaIter, Metadata},
    },
    redis::Commands,
    std::collections::HashMap,
};

#[async_trait(?Send)]
impl Metadata for RedisStorage {
    async fn scan_table_meta(&self) -> Result<MetaIter> {
        let mut all_metadata: HashMap<String, HashMap<String, Value>> = HashMap::new();
        let metadata_scan_key = Self::redis_generate_scan_all_metadata_key(&self.namespace);
        let redis_keys: Vec<String> = self
            .conn
            .borrow_mut()
            .scan_match(&metadata_scan_key)
            .map(|iter| iter.collect::<Vec<String>>())
            .map_err(|_| {
                Error::StorageMsg(format!(
                    "[RedisStorage] failed to scan metadata: namespace={}",
                    self.namespace
                ))
            })?;

        // Then read all values of the table
        for redis_key in redis_keys.into_iter() {
            // Another client just has removed the value with the key.
            // It's not a problem. Just ignore it.
            if let Ok(value) = redis::cmd("GET")
                .arg(&redis_key)
                .query::<String>(&mut self.conn.borrow_mut())
            {
                let value: Value = serde_json::from_str::<Value>(&value).map_err(|e| {
                    Error::StorageMsg(format!(
                        "[RedisStorage] failed to deserialize value: key={} error={}",
                        redis_key, e
                    ))
                })?;

                // [0]: namespace
                // [1]: 'metadata'
                // [2]: tablename
                // [3]: metadata_name
                let tokens = redis_key.split('#').collect::<Vec<&str>>();
                if let Some(meta_table) = all_metadata.get_mut(tokens[2]) {
                    meta_table.insert(tokens[3].to_owned(), value);
                } else {
                    let meta_table = HashMap::from([(tokens[3].to_owned(), value)]);
                    let meta = HashMap::from([(tokens[2].to_owned(), meta_table)]);
                    all_metadata.extend(meta);
                }
            }
        }

        Ok(Box::new(all_metadata.into_iter().map(Ok)))
    }
}
