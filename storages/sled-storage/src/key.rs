use sled::IVec;

const TEMP_DATA: &str = "temp_data/";
const TEMP_SCHEMA: &str = "temp_schema/";
const TEMP_INDEX: &str = "temp_index/";

pub fn data_prefix(table_name: &str) -> String {
    format!("data/{table_name}/")
}

pub fn data(table_name: &str, key: Vec<u8>) -> IVec {
    let key = data_prefix(table_name).into_bytes().into_iter().chain(key);

    IVec::from_iter(key)
}

macro_rules! prefix {
    ($txid: ident, $prefix: ident) => {
        $prefix
            .to_owned()
            .into_bytes()
            .into_iter()
            .chain($txid.to_be_bytes().iter().copied())
    };
}

pub fn temp_data_prefix(txid: u64) -> IVec {
    IVec::from_iter(prefix!(txid, TEMP_DATA))
}

pub fn temp_schema_prefix(txid: u64) -> IVec {
    IVec::from_iter(prefix!(txid, TEMP_SCHEMA))
}

pub fn temp_index_prefix(txid: u64) -> IVec {
    IVec::from_iter(prefix!(txid, TEMP_INDEX))
}

pub fn temp_data(txid: u64, data_key: &IVec) -> IVec {
    IVec::from_iter(prefix!(txid, TEMP_DATA).chain(data_key.iter().copied()))
}

pub fn temp_data_str(txid: u64, data_key: &str) -> IVec {
    IVec::from_iter(prefix!(txid, TEMP_DATA).chain(data_key.as_bytes().iter().copied()))
}

pub fn temp_schema(txid: u64, table_name: &str) -> IVec {
    IVec::from_iter(prefix!(txid, TEMP_SCHEMA).chain(table_name.as_bytes().iter().copied()))
}

pub fn temp_index(txid: u64, index_key: &[u8]) -> IVec {
    IVec::from_iter(prefix!(txid, TEMP_INDEX).chain(index_key.iter().copied()))
}
