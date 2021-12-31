use {
    super::{Key, MemoryStorage},
    async_trait::async_trait,
    gluesql_core::{
        ast::{IndexOperator, OrderByExpr},
        data::Value,
        result::{Error, MutResult, Result},
        store::{Index, IndexMut, RowIter},
    },
};

#[async_trait(?Send)]
impl Index<Key> for MemoryStorage {
    async fn scan_indexed_data(
        &self,
        _table_name: &str,
        _index_name: &str,
        _asc: Option<bool>,
        _cmp_value: Option<(&IndexOperator, Value)>,
    ) -> Result<RowIter<Key>> {
        Err(Error::StorageMsg(
            "[MemoryStorage] index is not supported".to_owned(),
        ))
    }
}

#[async_trait(?Send)]
impl IndexMut<Key> for MemoryStorage {
    async fn create_index(
        self,
        _table_name: &str,
        _index_name: &str,
        _column: &OrderByExpr,
    ) -> MutResult<Self, ()> {
        Err((
            self,
            Error::StorageMsg("[MemoryStorage] index is not supported".to_owned()),
        ))
    }

    async fn drop_index(self, _table_name: &str, _index_name: &str) -> MutResult<Self, ()> {
        Err((
            self,
            Error::StorageMsg("[MemoryStorage] index is not supported".to_owned()),
        ))
    }
}
