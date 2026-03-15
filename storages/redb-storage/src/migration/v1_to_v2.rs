use {
    crate::error::StorageError,
    bincode::{deserialize, serialize},
    gluesql_core::data::{Key, Value},
    redb::{ReadableTable, TableDefinition, WriteTransaction},
    serde::Deserialize,
    std::collections::BTreeMap,
};

type Result<T> = std::result::Result<T, StorageError>;
type PendingRewrite = (Vec<u8>, Vec<u8>);

const ROW_REWRITE_BATCH_SIZE: usize = 4096;

#[derive(Debug, Deserialize)]
enum V1DataRow {
    Vec(Vec<Value>),
    Map(BTreeMap<String, Value>),
}

pub(super) fn migrate_table_rows(txn: &WriteTransaction, table_name: &str) -> Result<usize> {
    let table_def = TableDefinition::<&[u8], Vec<u8>>::new(table_name);
    let mut rewritten_rows = 0;
    let mut last_scanned_key = None;

    loop {
        let (rewritten, batch_last_scanned_key, reached_end) =
            collect_rewrite_batch(txn, table_name, last_scanned_key.as_deref())?;

        if !rewritten.is_empty() {
            rewritten_rows += rewritten.len();

            let mut table = txn.open_table(table_def)?;
            for (table_key, value) in rewritten {
                table.insert(table_key.as_slice(), value)?;
            }
        }

        if reached_end {
            break;
        }

        last_scanned_key = batch_last_scanned_key;
        if last_scanned_key.is_none() {
            break;
        }
    }

    Ok(rewritten_rows)
}

fn collect_rewrite_batch(
    txn: &WriteTransaction,
    table_name: &str,
    last_scanned_key: Option<&[u8]>,
) -> Result<(Vec<PendingRewrite>, Option<Vec<u8>>, bool)> {
    use std::ops::Bound::{Excluded, Unbounded};

    let table_def = TableDefinition::<&[u8], Vec<u8>>::new(table_name);
    let table = txn.open_table(table_def)?;
    let iter = match last_scanned_key {
        Some(last_scanned_key) => table.range::<&[u8]>((Excluded(last_scanned_key), Unbounded))?,
        None => table.iter()?,
    };

    let mut rewritten = Vec::with_capacity(ROW_REWRITE_BATCH_SIZE);
    let mut batch_last_scanned_key = None;
    let mut reached_end = true;

    for entry in iter {
        let (table_key, value) = entry?;
        let table_key = table_key.value().to_vec();
        batch_last_scanned_key = Some(table_key.clone());

        let payload = value.value();
        if let Some(migrated) = migrate_row_payload(table_name, payload.as_slice())? {
            rewritten.push((table_key, migrated));
            if rewritten.len() == ROW_REWRITE_BATCH_SIZE {
                reached_end = false;
                break;
            }
        }
    }

    Ok((rewritten, batch_last_scanned_key, reached_end))
}

fn migrate_row_payload(table_name: &str, payload: &[u8]) -> Result<Option<Vec<u8>>> {
    if deserialize::<(Key, Vec<Value>)>(payload).is_ok() {
        return Ok(None);
    }

    let (key, row): (Key, V1DataRow) = deserialize(payload)
        .map_err(|_| StorageError::InvalidV1RowPayload(table_name.to_owned()))?;
    let row = match row {
        V1DataRow::Vec(values) => values,
        V1DataRow::Map(values) => vec![Value::Map(values)],
    };
    let migrated = serialize(&(&key, row))?;

    Ok(Some(migrated))
}
