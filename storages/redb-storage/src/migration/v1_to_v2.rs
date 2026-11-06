//! v1 -> v2: remove the `V1DataRow` envelope from each `(Key, V1DataRow)` payload.
//! `Vec(values)` becomes `values`; `Map(fields)` becomes `[Value::Map(fields)]`.
//! Schema bytes are preserved by this step.

use {
    super::{MigrationError, decode},
    crate::error::StorageError,
    bincode::serialize,
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
    if decode::deserialize::<(Key, Vec<Value>)>(payload).is_ok() {
        return Ok(None);
    }

    let (key, row): (Key, V1DataRow) = decode::deserialize(payload)
        .map_err(|_| MigrationError::InvalidV1RowPayload(table_name.to_owned()))?;
    let row = match row {
        V1DataRow::Vec(values) => values,
        V1DataRow::Map(values) => vec![Value::Map(values)],
    };
    let migrated = serialize(&(&key, row))?;

    Ok(Some(migrated))
}

#[cfg(test)]
mod tests {
    use {
        super::{Key, MigrationError, StorageError, Value, migrate_row_payload},
        std::collections::BTreeMap,
    };

    #[test]
    fn removes_the_data_row_vec_envelope() {
        let key = Key::I64(17);
        let values = vec![Value::I64(23), Value::Str("second".into())];
        // v1: (key, V1DataRow::Vec(values)), with Vec tag 0.
        let payload = bincode::serialize(&(&key, 0_u32, &values)).unwrap();
        let migrated = migrate_row_payload("Item", &payload).unwrap().unwrap();
        // v2: (key, values), without the enum tag.
        assert_eq!(migrated, bincode::serialize(&(key, values)).unwrap());
    }

    #[test]
    fn wraps_the_data_row_map_in_one_value() {
        let key = Key::I64(17);
        let fields = BTreeMap::from([("name".into(), Value::Str("Item".into()))]);
        // v1: (key, V1DataRow::Map(fields)), with Map tag 1.
        let payload = bincode::serialize(&(&key, 1_u32, &fields)).unwrap();
        let migrated = migrate_row_payload("Item", &payload).unwrap().unwrap();
        // v2: a schemaless row is a one-element Vec containing the whole map.
        assert_eq!(
            migrated,
            bincode::serialize(&(key, vec![Value::Map(fields)])).unwrap()
        );
    }

    #[test]
    fn empty_historical_vec_is_not_mistaken_for_a_current_row() {
        let key = Key::I64(1);
        let payload = bincode::serialize(&(&key, 0_u32, Vec::<Value>::new())).unwrap();
        let migrated = migrate_row_payload("Item", &payload).unwrap().unwrap();
        assert_eq!(
            migrated,
            bincode::serialize(&(key, Vec::<Value>::new())).unwrap()
        );
    }

    #[test]
    fn empty_historical_map_remains_a_schemaless_row() {
        let key = Key::I64(1);
        let payload = bincode::serialize(&(&key, 1_u32, BTreeMap::<String, Value>::new())).unwrap();
        let migrated = migrate_row_payload("Item", &payload).unwrap().unwrap();
        assert_eq!(
            bincode::deserialize::<(Key, Vec<Value>)>(&migrated).unwrap(),
            (key, vec![Value::Map(BTreeMap::new())])
        );
    }

    #[test]
    fn already_converted_rows_are_unchanged() {
        for values in [
            vec![],
            vec![Value::I64(100)],
            vec![Value::Map(BTreeMap::new())],
        ] {
            let payload = bincode::serialize(&(Key::I64(7), values)).unwrap();
            assert_eq!(migrate_row_payload("Item", &payload).unwrap(), None);
        }
    }

    #[test]
    fn invalid_rows_report_the_owning_table() {
        let mut trailing = bincode::serialize(&(Key::I64(1), vec![Value::I64(2)])).unwrap();
        trailing.push(255);
        for payload in [vec![], vec![255], vec![1, 2, 3, 4], trailing] {
            assert!(matches!(migrate_row_payload("Broken", &payload),
                Err(StorageError::Migration(MigrationError::InvalidV1RowPayload(name))) if name == "Broken"
            ));
        }
    }
}
