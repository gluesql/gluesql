use {
    super::MongoStorage,
    gluesql_core::store::{CustomFunction, CustomFunctionMut},
};

impl CustomFunction for MongoStorage {}
impl CustomFunctionMut for MongoStorage {}
