mod v1_to_v2;

use {
    crate::{FileStorage, ResultExt},
    gluesql_core::{
        data::Schema,
        error::{Error, Result},
    },
    std::{
        convert::AsRef,
        ffi::OsStr,
        fs,
        io::Write,
        path::{Path, PathBuf},
    },
    uuid::Uuid,
};

pub const FILE_STORAGE_FORMAT_VERSION: u32 = 2;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MigrationReport {
    pub migrated_tables: usize,
    pub unchanged_tables: usize,
    pub rewritten_rows: usize,
}

const V1_FILE_STORAGE_FORMAT_VERSION: u32 = 1;
const FORMAT_VERSION_PREFIX: &str = "-- gluesql:file-storage-format-version=";

#[derive(Debug)]
struct SchemaFile {
    version: Option<u32>,
    ddl: String,
}

impl FileStorage {
    pub(super) fn ensure_schema_versions_supported(path: &Path) -> Result<()> {
        for schema_path in list_schema_paths(path)? {
            let schema_file = read_schema_file(&schema_path)?;

            match schema_file.version {
                None => {
                    return Err(Error::StorageMsg(format!(
                        "[FileStorage] migration required for table schema '{}' (found v{V1_FILE_STORAGE_FORMAT_VERSION}, expected v{FILE_STORAGE_FORMAT_VERSION}); migrate file-storage data to the latest format before opening",
                        schema_path.display(),
                    )));
                }
                Some(FILE_STORAGE_FORMAT_VERSION) => {}
                Some(version) if version > FILE_STORAGE_FORMAT_VERSION => {
                    return Err(Error::StorageMsg(format!(
                        "[FileStorage] unsupported newer format version v{version} in schema '{}'",
                        schema_path.display(),
                    )));
                }
                Some(version) => {
                    return Err(Error::StorageMsg(format!(
                        "[FileStorage] unsupported format version v{version} in schema '{}'",
                        schema_path.display(),
                    )));
                }
            }
        }

        Ok(())
    }

    pub(super) fn write_schema_file(path: &Path, schema: &Schema) -> Result<()> {
        let data = format!(
            "{FORMAT_VERSION_PREFIX}{FILE_STORAGE_FORMAT_VERSION}\n{}",
            schema.to_ddl()
        );
        write_file_atomically(path, &data)
    }
}

pub fn migrate_to_latest<T: AsRef<Path>>(path: T) -> Result<MigrationReport> {
    let path = path.as_ref();
    if !path.exists() {
        return Err(Error::StorageMsg(format!(
            "[FileStorage] storage path '{}' does not exist",
            path.display()
        )));
    }
    if !path.is_dir() {
        return Err(Error::StorageMsg(format!(
            "[FileStorage] storage path '{}' is not a directory",
            path.display()
        )));
    }

    let mut report = MigrationReport::default();
    let schema_paths = list_schema_paths(path)?;

    for schema_path in schema_paths {
        let table_name = table_name_from_schema_path(&schema_path)?;
        let schema_file = read_schema_file(&schema_path)?;

        match schema_file.version {
            None => {
                let schema = Schema::from_ddl(&schema_file.ddl)?;
                v1_to_v2::migrate_table(path, &table_name, &schema_path, &schema, &mut report)?;
            }
            Some(FILE_STORAGE_FORMAT_VERSION) => {
                report.unchanged_tables += 1;
            }
            Some(version) if version > FILE_STORAGE_FORMAT_VERSION => {
                return Err(Error::StorageMsg(format!(
                    "[FileStorage] unsupported newer format version {version} for table '{table_name}'"
                )));
            }
            Some(version) => {
                return Err(Error::StorageMsg(format!(
                    "[FileStorage] unsupported format version v{version} for table '{table_name}'"
                )));
            }
        }
    }

    Ok(report)
}

fn list_schema_paths(path: &Path) -> Result<Vec<PathBuf>> {
    let mut schema_paths = fs::read_dir(path)
        .map_storage_err()?
        .map(|entry| {
            let entry = entry.map_storage_err()?;
            let file_type = entry.file_type().map_storage_err()?;
            let path = entry.path();
            let extension = path.extension().and_then(OsStr::to_str);
            Ok((file_type.is_file() && extension == Some("sql")).then_some(path))
        })
        .filter_map(Result::transpose)
        .collect::<Result<Vec<_>>>()?;
    schema_paths.sort();

    Ok(schema_paths)
}

fn read_schema_file(path: &Path) -> Result<SchemaFile> {
    let data = fs::read_to_string(path).map_storage_err()?;
    parse_schema_file(data)
}

fn write_file_atomically(path: &Path, data: &str) -> Result<()> {
    let temp_path = temp_path_for(path);
    let backup_path = backup_path_for(path);
    let has_existing_target = path.exists();

    let mut file = fs::File::create(&temp_path).map_storage_err()?;
    file.write_all(data.as_bytes()).map_storage_err()?;
    file.sync_all().map_storage_err()?;
    drop(file);

    if has_existing_target && let Err(backup_err) = fs::rename(path, &backup_path).map_storage_err()
    {
        let _ = fs::remove_file(&temp_path);
        return Err(backup_err);
    }

    if let Err(target_rename_err) = fs::rename(&temp_path, path).map_storage_err() {
        let _ = fs::remove_file(&temp_path);
        if has_existing_target
            && let Err(restore_err) = fs::rename(&backup_path, path).map_storage_err()
        {
            return Err(Error::StorageMsg(format!(
                "[FileStorage] failed to atomically replace '{}': {target_rename_err}; and failed to restore backup '{}': {restore_err}",
                path.display(),
                backup_path.display()
            )));
        }

        return Err(target_rename_err);
    }

    if has_existing_target {
        let _ = fs::remove_file(&backup_path);
    }

    Ok(())
}

fn temp_path_for(path: &Path) -> PathBuf {
    let extension = path.extension().and_then(OsStr::to_str).unwrap_or_default();
    let suffix = Uuid::now_v7();

    path.with_extension(format!("{extension}.tmp-{suffix}"))
}

fn backup_path_for(path: &Path) -> PathBuf {
    let extension = path.extension().and_then(OsStr::to_str).unwrap_or_default();
    let suffix = Uuid::now_v7();

    path.with_extension(format!("{extension}.bak-{suffix}"))
}

fn parse_schema_file(data: String) -> Result<SchemaFile> {
    if let Some(rest) = data.strip_prefix(FORMAT_VERSION_PREFIX) {
        let (version_line, ddl) = rest.split_once('\n').ok_or_else(|| {
            Error::StorageMsg(
                "[FileStorage] invalid schema format header: missing DDL after version marker"
                    .to_owned(),
            )
        })?;
        let version = version_line.trim().parse::<u32>().map_storage_err()?;

        return Ok(SchemaFile {
            version: Some(version),
            ddl: ddl.to_owned(),
        });
    }

    Ok(SchemaFile {
        version: None,
        ddl: data,
    })
}

fn table_name_from_schema_path(schema_path: &Path) -> Result<String> {
    schema_path
        .file_stem()
        .and_then(OsStr::to_str)
        .ok_or_else(|| {
            Error::StorageMsg(format!(
                "[FileStorage] failed to parse table name from '{}'",
                schema_path.display()
            ))
        })
        .map(ToOwned::to_owned)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn table_name_from_schema_path_requires_file_stem() {
        let err = table_name_from_schema_path(Path::new(""))
            .expect_err("path without file stem should fail");
        assert!(err.to_string().contains("failed to parse table name"));
    }
}
