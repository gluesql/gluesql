use sled::IVec;

pub const TEMP_DATA: &str = "temp_data/";
pub const TEMP_SCHEMA: &str = "temp_schema/";
pub const TEMP_INDEX: &str = "temp_index/";

pub fn temp_data(data_key: &IVec) -> IVec {
    let key = TEMP_DATA
        .to_owned()
        .into_bytes()
        .into_iter()
        .chain(data_key.iter().copied())
        .collect::<Vec<_>>();

    IVec::from(key)
}

#[cfg(feature = "alter-table")]
pub fn temp_data_str(data_key: &str) -> IVec {
    let key = format!("{}{}", TEMP_DATA, data_key);

    IVec::from(key.into_bytes())
}

pub fn temp_schema(table_name: &str) -> IVec {
    let key = format!("{}{}", TEMP_SCHEMA, table_name);

    IVec::from(key.into_bytes())
}

pub fn temp_index(index_key: &[u8]) -> IVec {
    let key = TEMP_INDEX
        .to_owned()
        .into_bytes()
        .into_iter()
        .chain(index_key.iter().copied())
        .collect::<Vec<_>>();

    IVec::from(key)
}
