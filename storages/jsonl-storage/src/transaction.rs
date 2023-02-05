#![cfg(feature = "transaction")]
use {super::JsonlStorage, gluesql_core::store::Transaction};

impl Transaction for JsonlStorage {}
