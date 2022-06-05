use {
    crate::prelude::DataType,
    serde::{Deserialize, Serialize},
};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CustomType {
    name: String,
    fields: Vec<CustomTypeField>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct CustomTypeField {
    name: String,
    data_type: DataType,
}
