use {
    super::JsonStorage,
    gluesql_core::store::{CustomFunction, CustomFunctionMut},
};

impl CustomFunction for JsonStorage {}
impl CustomFunctionMut for JsonStorage {}
