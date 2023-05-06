use {
    super::MemoryStorage,
    async_trait::async_trait,
    gluesql_core::{
        ast::{IndexOperator, OrderByExpr},
        data::Value,
        error::{Error, Result},
        store::{Index, IndexMut, RowIter},
    },
};

#[async_trait(?Send)]
impl Index for MemoryStorage {
    async fn scan_indexed_data(
        &self,
        _table_name: &str,
        _index_name: &str,
        _asc: Option<bool>,
        _cmp_value: Option<(&IndexOperator, Value)>,
    ) -> Result<RowIter> {
        Err(Error::StorageMsg(
            "[MemoryStorage] index is not supported".to_owned(),
        ))
    }
}

#[async_trait(?Send)]
impl IndexMut for MemoryStorage {
    async fn create_index(
        &mut self,
        _table_name: &str,
        _index_name: &str,
        _column: &OrderByExpr,
    ) -> Result<()> {
        Err(Error::StorageMsg(
            "[MemoryStorage] index is not supported".to_owned(),
        ))
    }

    async fn drop_index(&mut self, _table_name: &str, _index_name: &str) -> Result<()> {
        Err(Error::StorageMsg(
            "[MemoryStorage] index is not supported".to_owned(),
        ))
    }
}
