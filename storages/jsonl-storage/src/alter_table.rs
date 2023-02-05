#![cfg(feature = "alter-table")]
use {super::JsonlStorage, gluesql_core::store::AlterTable};

impl AlterTable for JsonlStorage {}
