#[cfg(feature = "index")]
#[macro_export]
macro_rules! impl_default_for_index {
    ($storage_name: ident) => {
        #[cfg(feature = "index")]
        #[async_trait]
        impl Index for $storage_name {
            async fn scan_indexed_data(
                &self,
                _table_name: &str,
                _index_name: &str,
                _asc: Option<bool>,
                _cmp_value: Option<(&IndexOperator, Value)>,
            ) -> Result<RowIter> {
                Err(Error::StorageMsg(format!(
                    "[{}] Index::scan_indexed_data is not supported",
                    stringify!($storage_name)
                )))
            }
        }

        #[cfg(feature = "index")]
        #[async_trait]
        impl IndexMut for $storage_name {
            async fn create_index(
                &mut self,
                _table_name: &str,
                _index_name: &str,
                _column: &OrderByExpr,
            ) -> Result<()> {
                Err(Error::StorageMsg(format!(
                    "[{}] Index::create_index is not supported",
                    stringify!($storage_name)
                )))
            }

            async fn drop_index(&mut self, _table_name: &str, _index_name: &str) -> Result<()> {
                Err(Error::StorageMsg(format!(
                    "[{}] Index::drop_index is not supported",
                    stringify!($storage_name)
                )))
            }
        }
    };
}

#[cfg(feature = "alter-table")]
#[macro_export]
macro_rules! impl_default_for_alter_table {
    ($storage_name: ident) => {
        #[cfg(feature = "alter-table")]
        #[async_trait]
        impl AlterTable for $storage_name {
            async fn rename_schema(
                &mut self,
                _table_name: &str,
                _new_table_name: &str,
            ) -> Result<()> {
                {
                    Err(Error::StorageMsg(format!(
                        "
						[{}] AlterTable::rename_schema is not supported",
                        stringify!($storage_name)
                    )))
                }
            }

            async fn rename_column(
                &mut self,
                _table_name: &str,
                _old_column_name: &str,
                _new_column_name: &str,
            ) -> Result<()> {
                Err(Error::StorageMsg(format!(
                    "[{}] AlterTable::rename_column is not supported",
                    stringify!($storage_name),
                )))
            }

            async fn add_column(
                &mut self,
                _table_name: &str,
                _column_def: &ColumnDef,
            ) -> Result<()> {
                Err(Error::StorageMsg(format!(
                    "[{}] AlterTable::add_column is not supported",
                    stringify!($storage_name),
                )))
            }

            async fn drop_column(
                &mut self,
                _table_name: &str,
                _column_name: &str,
                _if_exists: bool,
            ) -> Result<()> {
                Err(Error::StorageMsg(format!(
                    "[{}] AlterTable::drop_column is not supported",
                    stringify!($storage_name)
                )))
            }
        }
    };
}

#[macro_export]
macro_rules! impl_default_for_transaction {
    ($storage_name: ident) => {
        #[cfg(feature = "transaction")]
        #[async_trait]
        impl Transaction for $storage_name {
            async fn begin(&mut self, autocommit: bool) -> Result<bool> {
                if autocommit {
                    return Ok(false);
                }

                Err(Error::StorageMsg(format!(
                    "[{}] Transaction::begin is not supported",
                    stringify!($storage_name)
                )))
            }

            async fn rollback(&mut self) -> Result<()> {
                Err(Error::StorageMsg(format!(
                    "[{}] Transaction::rollback is not supported",
                    stringify!($storage_name)
                )))
            }

            async fn commit(&mut self) -> Result<()> {
                Err(Error::StorageMsg(format!(
                    "[{}] Transaction::commit is not supported",
                    stringify!($storage_name)
                )))
            }
        }
    };
}
