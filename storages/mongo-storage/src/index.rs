use {
    super::MongoStorage,
    gluesql_core::store::{Index, IndexMut},
};

impl Index for MongoStorage {}
impl IndexMut for MongoStorage {}
