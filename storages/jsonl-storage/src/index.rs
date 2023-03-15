use {
    super::JsonlStorage,
    gluesql_core::store::{Index, IndexMut},
};

impl Index for JsonlStorage {}
impl IndexMut for JsonlStorage {}
