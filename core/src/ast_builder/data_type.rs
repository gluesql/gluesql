use crate::{
    ast::DataType,
    parse_sql::parse_data_type,
    result::{Error, Result},
    translate::translate_data_type,
};

#[derive(Clone, Debug)]
pub enum DataTypeNode {
    DataType(DataType),
    Text(String),
}

impl From<DataType> for DataTypeNode {
    fn from(data_type: DataType) -> Self {
        Self::DataType(data_type)
    }
}

impl From<&str> for DataTypeNode {
    fn from(data_type: &str) -> Self {
        Self::Text(data_type.to_owned())
    }
}

impl TryFrom<DataTypeNode> for DataType {
    type Error = Error;

    fn try_from(data_type: DataTypeNode) -> Result<Self> {
        match data_type {
            DataTypeNode::DataType(data_type) => Ok(data_type),
            DataTypeNode::Text(data_type) => {
                parse_data_type(data_type).and_then(|datatype| translate_data_type(&datatype))
            }
        }
    }
}
