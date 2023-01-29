#![cfg(feature = "index")]

use {
    super::SharedMemoryStorage,
    async_trait::async_trait,
    gluesql_core::{
        ast::{IndexOperator, OrderByExpr},
        data::Value,
        result::{Error, Result},
        store::{Index, IndexMut, RowIter},
    },
};

gluesql_core::impl_default_for_index!(SharedMemoryStorage);
