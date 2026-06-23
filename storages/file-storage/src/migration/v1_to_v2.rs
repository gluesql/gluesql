use {
    super::{MigrationReport, write_file_atomically},
    crate::{FileRow, FileStorage, ResultExt},
    gluesql_core::{
        data::{Key, Schema, Value},
        error::{Error, Result},
    },
    ron::ser::{PrettyConfig, to_string_pretty},
    serde::Deserialize,
    std::{collections::BTreeMap, ffi::OsStr, fs, path::Path},
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

pub(super) fn migrate_table(
    path: &Path,
    table_name: &str,
    schema_path: &Path,
    schema: &Schema,
    report: &mut MigrationReport,
) -> Result<()> {
    migrate_rows(path, table_name, report)?;
    FileStorage::write_schema_file(schema_path, schema)?;
    report.migrated_tables += 1;

    Ok(())
}

fn migrate_rows(path: &Path, table_name: &str, report: &mut MigrationReport) -> Result<()> {
    let table_path = path.join(table_name);
    if !table_path.exists() {
        return Ok(());
    }

    let mut row_paths = fs::read_dir(table_path)
        .map_storage_err()?
        .map(|entry| {
            let entry = entry.map_storage_err()?;
            let file_type = entry.file_type().map_storage_err()?;
            let path = entry.path();
            let extension = path.extension().and_then(OsStr::to_str);
            Ok((file_type.is_file() && extension == Some("ron")).then_some(path))
        })
        .filter_map(Result::transpose)
        .collect::<Result<Vec<_>>>()?;
    row_paths.sort();

    for row_path in row_paths {
        let data = fs::read_to_string(&row_path).map_storage_err()?;

        if ron::from_str::<FileRow>(&data).is_ok() {
            continue;
        }

        let row = if let Ok(V1EnumFileRow { key, row }) = ron::from_str(&data) {
            let row = match row {
                V1DataRowEnum::Vec(values) => values,
                V1DataRowEnum::Map(map) => vec![Value::Map(map)],
            };

            FileRow { key, row }
        } else if let Ok(V1WrappedFileRow {
            key,
            row: V1DataRowWrapped(values),
        }) = ron::from_str(&data)
        {
            FileRow { key, row: values }
        } else {
            return Err(Error::StorageMsg(format!(
                "[FileStorage] failed to parse v1 row file '{}'",
                row_path.display()
            )));
        };
        let serialized = to_string_pretty(&row, PrettyConfig::default()).map_storage_err()?;
        write_file_atomically(&row_path, &serialized)?;
        report.rewritten_rows += 1;
    }

    Ok(())
}
