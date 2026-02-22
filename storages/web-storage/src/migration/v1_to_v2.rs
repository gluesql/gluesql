use {
    crate::{DATA_PATH, TABLE_NAMES_PATH, WebStorage},
    gluesql_core::{
        data::{Key, Value},
        error::{Error, Result},
    },
    serde::Deserialize,
    std::collections::BTreeMap,
};

#[derive(Debug, Deserialize)]
enum V1DataRow {
    Vec(Vec<Value>),
    Map(BTreeMap<String, Value>),
}

pub(super) fn migrate(storage: &WebStorage) -> Result<()> {
    for table_name in storage
        .get::<Vec<String>>(TABLE_NAMES_PATH)?
        .unwrap_or_default()
    {
        migrate_table_rows(storage, &table_name)?;
    }

    Ok(())
}

fn migrate_table_rows(storage: &WebStorage, table_name: &str) -> Result<()> {
    let path = format!("{DATA_PATH}/{table_name}");
    let Some(raw_rows) = storage.get::<serde_json::Value>(&path)? else {
        return Ok(());
    };

    if serde_json::from_value::<Vec<(Key, Vec<Value>)>>(raw_rows.clone()).is_ok() {
        return Ok(());
    }

    let rows = serde_json::from_value::<Vec<(Key, V1DataRow)>>(raw_rows).map_err(|_| {
        Error::StorageMsg(format!(
            "[WebStorage] conflict - failed to parse v1 row payload in table '{table_name}'"
        ))
    })?;
    let rows = rows
        .into_iter()
        .map(|(key, row)| {
            let row = match row {
                V1DataRow::Vec(values) => values,
                V1DataRow::Map(values) => vec![Value::Map(values)],
            };

            (key, row)
        })
        .collect::<Vec<_>>();

    storage.set(path, rows)
}
