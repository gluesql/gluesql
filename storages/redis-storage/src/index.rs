use {
    super::RedisStorage,
    gluesql_core::{
        ast::{IndexOperator, OrderByExpr},
        data::Value,
        error::{Error, Result},
        store::{Index, IndexMut, RowIter},
    },
};

// Index is one of MUST-be-implemented traits.

impl Index for RedisStorage {
    fn scan_indexed_data<'a>(
        &'a self,
        _table_name: &str,
        _index_name: &str,
        _asc: Option<bool>,
        _cmp_value: Option<(&IndexOperator, Value)>,
    ) -> Result<RowIter<'a>> {
        Err(Error::StorageMsg(
            "[RedisStorage] index is not supported".to_owned(),
        ))
    }
}

impl IndexMut for RedisStorage {
    fn create_index(
        &mut self,
        _table_name: &str,
        _index_name: &str,
        _column: &OrderByExpr,
    ) -> Result<()> {
        Err(Error::StorageMsg(
            "[RedisStorage] index is not supported".to_owned(),
        ))
    }

    fn drop_index(&mut self, _table_name: &str, _index_name: &str) -> Result<()> {
        Err(Error::StorageMsg(
            "[RedisStorage] index is not supported".to_owned(),
        ))
    }
}
