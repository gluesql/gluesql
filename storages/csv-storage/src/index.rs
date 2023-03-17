use {
    crate::CsvStorage,
    gluesql_core::store::{Index, IndexMut},
};

impl Index for CsvStorage {}
impl IndexMut for CsvStorage {}
