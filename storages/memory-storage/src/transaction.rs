#![cfg(feature = "transaction")]

use {
    super::MemoryStorage,
    async_trait::async_trait,
    gluesql_core::{
        result::{Error, Result},
        store::Transaction,
    },
};

gluesql_core::impl_default_for_transaction!(MemoryStorage);
