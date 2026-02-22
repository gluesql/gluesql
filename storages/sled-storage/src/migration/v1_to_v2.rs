use {
    crate::err_into,
    gluesql_core::{
        data::Value,
        error::{Error, Result},
    },
    serde::{Deserialize, Serialize},
    sled::{Db, IVec},
    std::collections::BTreeMap,
};

const DATA_PREFIX: &str = "data/";

#[derive(Debug, Clone, Serialize, Deserialize)]
enum V1DataRow {
    Vec(Vec<Value>),
    Map(BTreeMap<String, Value>),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct V1VecDataRow(Vec<Value>);

#[derive(Debug, Clone, Serialize, Deserialize)]
struct V1SnapshotItem<T> {
    data: T,
    created_by: u64,
    deleted_by: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct V1Snapshot<T>(Vec<V1SnapshotItem<T>>);

#[derive(Debug, Clone, Serialize, Deserialize)]
struct V2SnapshotItem<T> {
    data: T,
    created_by: u64,
    deleted_by: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct V2Snapshot<T>(Vec<V2SnapshotItem<T>>);

pub fn migrate_tree(tree: &Db) -> Result<usize> {
    let rewritten_rows = tree
        .scan_prefix(DATA_PREFIX)
        .map(|item| {
            let (data_key, data_value) = item.map_err(err_into)?;
            let row_snapshot = deserialize_v1_row_snapshot(&data_value)?;
            let row_snapshot = bincode::serialize(&row_snapshot).map_err(err_into)?;

            Ok((data_key, row_snapshot))
        })
        .collect::<Result<Vec<(IVec, Vec<u8>)>>>()?;
    let rewritten_rows_len = rewritten_rows.len();

    for (data_key, row_snapshot) in rewritten_rows {
        tree.insert(data_key, row_snapshot).map_err(err_into)?;
    }

    if rewritten_rows_len > 0 {
        tree.flush().map_err(err_into)?;
    }

    Ok(rewritten_rows_len)
}

fn deserialize_v1_row_snapshot(data: &[u8]) -> Result<V2Snapshot<Vec<Value>>> {
    if let Ok(snapshot) = bincode::deserialize::<V1Snapshot<V1DataRow>>(data) {
        return Ok(snapshot.try_map_data(|v1_row| match v1_row {
            V1DataRow::Vec(values) => values,
            V1DataRow::Map(values) => vec![Value::Map(values)],
        }));
    }

    if let Ok(snapshot) = bincode::deserialize::<V1Snapshot<V1VecDataRow>>(data) {
        return Ok(snapshot.try_map_data(|V1VecDataRow(values)| values));
    }

    if let Ok(snapshot) = bincode::deserialize::<V2Snapshot<Vec<Value>>>(data) {
        return Ok(snapshot);
    }

    bincode::deserialize::<V1Snapshot<Vec<Value>>>(data)
        .map(|snapshot| snapshot.try_map_data(std::convert::identity))
        .map_err(|parse_err| {
            Error::StorageMsg(format!(
                "[SledStorage] failed to parse v1 row snapshot during migration: {parse_err}",
            ))
        })
}

impl<T> V1Snapshot<T> {
    fn try_map_data<U, F>(self, mut mapper: F) -> V2Snapshot<U>
    where
        F: FnMut(T) -> U,
    {
        let items = self
            .0
            .into_iter()
            .map(
                |V1SnapshotItem {
                     data,
                     created_by,
                     deleted_by,
                 }| V2SnapshotItem {
                    data: mapper(data),
                    created_by,
                    deleted_by,
                },
            )
            .collect();

        V2Snapshot(items)
    }
}
