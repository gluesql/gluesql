use {
    super::{FILE_STORAGE_FORMAT_VERSION, atomic_file},
    crate::ResultExt,
    gluesql_core::{
        data::Schema,
        error::{Error, Result},
    },
    std::{
        ffi::OsStr,
        fs,
        path::{Path, PathBuf},
    },
};

const V1_FILE_STORAGE_FORMAT_VERSION: u32 = 1;
const FORMAT_VERSION_PREFIX: &str = "-- gluesql:file-storage-format-version=";

#[derive(Debug)]
pub(super) struct SchemaFile {
    pub(super) version: Option<u32>,
    pub(super) ddl: String,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) struct TableVersions {
    pub(super) v1: usize,
    pub(super) latest: usize,
}

pub(super) fn read(path: &Path) -> Result<SchemaFile> {
    let data = fs::read_to_string(path).map_storage_err()?;

    parse(data)
}

pub(super) fn write(path: &Path, schema: &Schema) -> Result<()> {
    let data = format!(
        "{FORMAT_VERSION_PREFIX}{FILE_STORAGE_FORMAT_VERSION}\n{}",
        schema.to_ddl()
    );

    atomic_file::write(path, &data)
}

pub(super) fn list_paths(root: &Path) -> Result<Vec<PathBuf>> {
    let mut schema_paths = fs::read_dir(root)
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

pub(super) fn table_name(schema_path: &Path) -> Result<String> {
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

pub(super) fn classify(root: &Path) -> Result<TableVersions> {
    list_paths(root)?
        .into_iter()
        .try_fold(TableVersions::default(), |versions, schema_path| {
            match read(&schema_path)?.version {
                None => Ok(TableVersions {
                    v1: versions.v1 + 1,
                    ..versions
                }),
                Some(FILE_STORAGE_FORMAT_VERSION) => Ok(TableVersions {
                    latest: versions.latest + 1,
                    ..versions
                }),
                Some(version) => {
                    let table_name = table_name(&schema_path)?;
                    let qualifier = if version > FILE_STORAGE_FORMAT_VERSION {
                        "newer "
                    } else {
                        ""
                    };

                    Err(Error::StorageMsg(format!(
                        "[FileStorage] unsupported {qualifier}format version v{version} for table '{table_name}'"
                    )))
                }
            }
        })
}

pub(super) fn ensure_versions_supported(root: &Path) -> Result<()> {
    for schema_path in list_paths(root)? {
        match read(&schema_path)?.version {
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

fn parse(data: String) -> Result<SchemaFile> {
    let Some(rest) = data.strip_prefix(FORMAT_VERSION_PREFIX) else {
        return Ok(SchemaFile {
            version: None,
            ddl: data,
        });
    };

    let (version_line, ddl) = rest.split_once('\n').ok_or_else(|| {
        Error::StorageMsg(
            "[FileStorage] invalid schema format header: missing DDL after version marker"
                .to_owned(),
        )
    })?;

    Ok(SchemaFile {
        version: Some(version_line.trim().parse::<u32>().map_storage_err()?),
        ddl: ddl.to_owned(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn table_name_requires_a_file_stem() {
        let err = table_name(Path::new("")).expect_err("path without file stem should fail");
        assert!(err.to_string().contains("failed to parse table name"));
    }
}
