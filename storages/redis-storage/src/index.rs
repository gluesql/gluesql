use {
    super::RedisStorage,
    async_trait::async_trait,
    gluesql_core::{
        ast::{IndexOperator, OrderByExpr},
        data::Value,
        error::{Error, Result},
        store::{Index, IndexMut, RowIter},
    },
};

// Index is one of MUST-be-implemented traits.

#[async_trait(?Send)]
impl Index for RedisStorage {
    async fn scan_indexed_data(
        &self,
        _table_name: &str,
        _index_name: &str,
        _asc: Option<bool>,
        _cmp_value: Option<(&IndexOperator, Value)>,
    ) -> Result<RowIter> {
        Err(Error::StorageMsg(
            "[RedisStorage] index is not supported".to_owned(),
        ))
    }
}

#[async_trait(?Send)]
impl IndexMut for RedisStorage {
    async fn create_index(
        &mut self,
        _table_name: &str,
        _index_name: &str,
        _column: &OrderByExpr,
    ) -> Result<()> {
        Err(Error::StorageMsg(
            "[RedisStorage] index is not supported".to_owned(),
        ))
    }

    async fn drop_index(&mut self, _table_name: &str, _index_name: &str) -> Result<()> {
        Err(Error::StorageMsg(
            "[RedisStorage] index is not supported".to_owned(),
        ))
    }
}
