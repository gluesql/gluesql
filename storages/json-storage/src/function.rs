use {
    super::JsonStorage,
    gluesql_core::store::{Function, FunctionMut},
};

impl Function for JsonStorage {}
impl FunctionMut for JsonStorage {}