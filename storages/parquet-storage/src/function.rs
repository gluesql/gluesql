use {
    super::ParquetStorage,
    gluesql_core::store::{CustomFunction, CustomFunctionMut},
};

impl CustomFunctionMut for ParquetStorage {}
impl CustomFunction for ParquetStorage {}
