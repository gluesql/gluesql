use {
    super::SharedMemoryStorage,
    gluesql_core::{
        ast::{IndexOperator, OrderByExpr},
        data::Value,
        error::{Error, Result},
        store::{Index, IndexMut, RowIter},
    },
};

impl Index for SharedMemoryStorage {
    fn scan_indexed_data<'a>(
        &'a self,
        _table_name: &str,
        _index_name: &str,
        _asc: Option<bool>,
        _cmp_value: Option<(&IndexOperator, Value)>,
    ) -> Result<RowIter<'a>> {
        Err(Error::StorageMsg(
            "[Shared MemoryStorage] index is not supported".to_owned(),
        ))
    }
}

impl IndexMut for SharedMemoryStorage {
    fn create_index(
        &mut self,
        _table_name: &str,
        _index_name: &str,
        _column: &OrderByExpr,
    ) -> Result<()> {
        Err(Error::StorageMsg(
            "[Shared MemoryStorage] index is not supported".to_owned(),
        ))
    }

    fn drop_index(&mut self, _table_name: &str, _index_name: &str) -> Result<()> {
        Err(Error::StorageMsg(
            "[Shared MemoryStorage] index is not supported".to_owned(),
        ))
    }
}
