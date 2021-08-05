use sled::IVec;

const TEMP_DATA: &str = "temp_data/";
const TEMP_SCHEMA: &str = "temp_schema/";
const TEMP_INDEX: &str = "temp_index/";

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
    IVec::from(prefix!(txid, TEMP_DATA).collect::<Vec<_>>())
}

pub fn temp_schema_prefix(txid: u64) -> IVec {
    IVec::from(prefix!(txid, TEMP_SCHEMA).collect::<Vec<_>>())
}

pub fn temp_index_prefix(txid: u64) -> IVec {
    IVec::from(prefix!(txid, TEMP_INDEX).collect::<Vec<_>>())
}

pub fn temp_data(txid: u64, data_key: &IVec) -> IVec {
    let key = prefix!(txid, TEMP_DATA)
        .chain(data_key.iter().copied())
        .collect::<Vec<_>>();

    IVec::from(key)
}

#[cfg(feature = "alter-table")]
pub fn temp_data_str(txid: u64, data_key: &str) -> IVec {
    let key = prefix!(txid, TEMP_DATA)
        .chain(data_key.as_bytes().iter().copied())
        .collect::<Vec<_>>();

    IVec::from(key)
}

pub fn temp_schema(txid: u64, table_name: &str) -> IVec {
    let key = prefix!(txid, TEMP_SCHEMA)
        .chain(table_name.as_bytes().iter().copied())
        .collect::<Vec<_>>();

    IVec::from(key)
}

pub fn temp_index(txid: u64, index_key: &[u8]) -> IVec {
    let key = prefix!(txid, TEMP_INDEX)
        .chain(index_key.iter().copied())
        .collect::<Vec<_>>();

    IVec::from(key)
}
