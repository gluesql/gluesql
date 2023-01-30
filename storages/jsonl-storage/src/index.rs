#![cfg(feature = "index")]

use {
    super::JsonlStorage,
    async_trait::async_trait,
    gluesql_core::{
        ast::{IndexOperator, OrderByExpr},
        data::Value,
        result::{Error, Result},
        store::{Index, IndexMut, RowIter},
    },
};

#[async_trait(?Send)]
impl Index for JsonlStorage {
    async fn scan_indexed_data(
        &self,
        _table_name: &str,
        _index_name: &str,
        _asc: Option<bool>,
        _cmp_value: Option<(&IndexOperator, Value)>,
    ) -> Result<RowIter> {
        Err(Error::StorageMsg(
            "[JsonlStorage] index is not supported".to_owned(),
        ))
    }
}

#[async_trait(?Send)]
impl IndexMut for JsonlStorage {
    async fn create_index(
        &mut self,
        _table_name: &str,
        _index_name: &str,
        _column: &OrderByExpr,
    ) -> Result<()> {
        Err(Error::StorageMsg(
            "[JsonlStorage] index is not supported".to_owned(),
        ))
    }

    async fn drop_index(&mut self, _table_name: &str, _index_name: &str) -> Result<()> {
        Err(Error::StorageMsg(
            "[JsonlStorage] index is not supported".to_owned(),
        ))
    }
}
