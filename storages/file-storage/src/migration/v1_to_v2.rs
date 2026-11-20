use {
    super::staging::SourceRow,
    crate::{FileRow, ResultExt},
    gluesql_core::{
        data::{Key, Value},
        error::{Error, Result},
    },
    serde::Deserialize,
    std::{collections::BTreeMap, fs, path::Path},
};

#[derive(Debug, Deserialize)]
struct V1EnumFileRow {
    key: Key,
    row: V1DataRowEnum,
}

#[derive(Debug, Deserialize)]
enum V1DataRowEnum {
    Vec(Vec<Value>),
    Map(BTreeMap<String, Value>),
}

#[derive(Debug, Deserialize)]
struct V1DataRowWrapped(Vec<Value>);

#[derive(Debug, Deserialize)]
struct V1WrappedFileRow {
    key: Key,
    row: V1DataRowWrapped,
}

pub(super) fn decode_row(row_path: &Path) -> Result<SourceRow> {
    let data = fs::read_to_string(row_path).map_storage_err()?;

    if let Ok(FileRow { key, row }) = ron::from_str::<FileRow>(&data) {
        return Ok(SourceRow {
            key,
            values: row,
            converted: false,
        });
    }

    if let Ok(V1EnumFileRow { key, row }) = ron::from_str(&data) {
        let values = match row {
            V1DataRowEnum::Vec(values) => values,
            V1DataRowEnum::Map(map) => vec![Value::Map(map)],
        };

        return Ok(SourceRow {
            key,
            values,
            converted: true,
        });
    }

    if let Ok(V1WrappedFileRow {
        key,
        row: V1DataRowWrapped(values),
    }) = ron::from_str(&data)
    {
        return Ok(SourceRow {
            key,
            values,
            converted: true,
        });
    }

    Err(Error::StorageMsg(format!(
        "[FileStorage] failed to parse v1 row file '{}'; no data was modified",
        row_path.display()
    )))
}
