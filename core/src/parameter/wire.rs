use {
    super::ParameterError,
    crate::data::Value,
    serde_json::{from_slice, to_vec},
};

impl TryFrom<&Vec<u8>> for Value {
    type Error = ParameterError;
    fn try_from(bytes: &Vec<u8>) -> Result<Self, Self::Error> {
        // TODO: recover Value from bytes. use serde_json for now.
        Ok(from_slice(&bytes[..])?)
    }
}

impl TryFrom<Vec<u8>> for Value {
    type Error = ParameterError;
    fn try_from(bytes: Vec<u8>) -> Result<Self, Self::Error> {
        (&bytes).try_into()
    }
}

impl Into<Vec<u8>> for &Value {
    fn into(self) -> Vec<u8> {
        to_vec(self).unwrap()
    }
}
