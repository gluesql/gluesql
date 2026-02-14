mod v1_to_v2;

use {
    crate::{Snapshot, err_into},
    gluesql_core::{
        data::Schema,
        error::{Error, Result},
    },
    sled::Db,
    std::path::Path,
};

pub const SLED_STORAGE_FORMAT_VERSION: u32 = 2;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MigrationReport {
    pub migrated_tables: usize,
    pub unchanged_tables: usize,
    pub rewritten_rows: usize,
}

const V1_SLED_STORAGE_FORMAT_VERSION: u32 = 1;
const SCHEMA_PREFIX: &str = "schema/";
const STORAGE_FORMAT_VERSION_KEY: &str = "__GLUESQL_STORAGE_FORMAT_VERSION__";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DetectedStorageFormatVersion {
    V1,
    V2,
    UnsupportedNewer(u32),
    Unsupported(u32),
}

pub(super) fn ensure_storage_format_version_supported(tree: &Db) -> Result<()> {
    match detect_storage_format_version(tree)? {
        DetectedStorageFormatVersion::V1 => Err(Error::StorageMsg(format!(
            "[SledStorage] migration required (found v{V1_SLED_STORAGE_FORMAT_VERSION}, expected v{SLED_STORAGE_FORMAT_VERSION}); migrate sled-storage data to the latest format before opening",
        ))),
        DetectedStorageFormatVersion::V2 => Ok(()),
        DetectedStorageFormatVersion::UnsupportedNewer(version) => Err(Error::StorageMsg(format!(
            "[SledStorage] unsupported newer format version v{version}"
        ))),
        DetectedStorageFormatVersion::Unsupported(version) => Err(Error::StorageMsg(format!(
            "[SledStorage] unsupported format version v{version}",
        ))),
    }
}

pub(super) fn initialize_storage_format_version(tree: &Db) -> Result<()> {
    write_storage_format_version(tree, SLED_STORAGE_FORMAT_VERSION)
}

pub(super) fn prepare_import_destination(tree: &Db) -> Result<()> {
    let mut iter = tree.iter();
    let first = iter.next().transpose().map_err(err_into)?;

    let Some((first_key, _)) = first else {
        return Ok(());
    };

    let has_more_keys = iter.next().transpose().map_err(err_into)?.is_some();
    if !has_more_keys && first_key.as_ref() == STORAGE_FORMAT_VERSION_KEY.as_bytes() {
        tree.remove(STORAGE_FORMAT_VERSION_KEY).map_err(err_into)?;
        return Ok(());
    }

    Err(Error::StorageMsg(
        "[SledStorage] import requires an empty destination storage".to_owned(),
    ))
}

pub fn migrate_to_latest<P: AsRef<Path>>(path: P) -> Result<MigrationReport> {
    let path = path.as_ref();
    if !path.exists() {
        return Err(Error::StorageMsg(format!(
            "[SledStorage] storage path '{}' does not exist",
            path.display()
        )));
    }
    if !path.is_dir() {
        return Err(Error::StorageMsg(format!(
            "[SledStorage] storage path '{}' is not a directory",
            path.display()
        )));
    }

    let tree = sled::open(path).map_err(err_into)?;
    migrate_tree_to_latest(&tree)
}

fn migrate_tree_to_latest(tree: &Db) -> Result<MigrationReport> {
    match detect_storage_format_version(tree)? {
        DetectedStorageFormatVersion::V1 => {
            let migrated_tables = list_alive_table_names(tree)?.len();
            let rewritten_rows = v1_to_v2::migrate_tree(tree)?;

            write_storage_format_version(tree, SLED_STORAGE_FORMAT_VERSION)?;

            Ok(MigrationReport {
                migrated_tables,
                unchanged_tables: 0,
                rewritten_rows,
            })
        }
        DetectedStorageFormatVersion::V2 => {
            let unchanged_tables = list_alive_table_names(tree)?.len();

            Ok(MigrationReport {
                migrated_tables: 0,
                unchanged_tables,
                rewritten_rows: 0,
            })
        }
        DetectedStorageFormatVersion::UnsupportedNewer(version) => Err(Error::StorageMsg(format!(
            "[SledStorage] unsupported newer format version v{version}"
        ))),
        DetectedStorageFormatVersion::Unsupported(version) => Err(Error::StorageMsg(format!(
            "[SledStorage] unsupported format version v{version}",
        ))),
    }
}

fn detect_storage_format_version(tree: &Db) -> Result<DetectedStorageFormatVersion> {
    let Some(value) = tree
        .get(STORAGE_FORMAT_VERSION_KEY)
        .map_err(err_into)?
        .map(|v| v.to_vec())
    else {
        return Ok(DetectedStorageFormatVersion::V1);
    };

    let version = parse_storage_format_version(&value)?;

    Ok(match version {
        SLED_STORAGE_FORMAT_VERSION => DetectedStorageFormatVersion::V2,
        version if version > SLED_STORAGE_FORMAT_VERSION => {
            DetectedStorageFormatVersion::UnsupportedNewer(version)
        }
        version => DetectedStorageFormatVersion::Unsupported(version),
    })
}

fn parse_storage_format_version(value: &[u8]) -> Result<u32> {
    let bytes: [u8; 4] = value.try_into().map_err(|_| {
        Error::StorageMsg(format!(
            "[SledStorage] invalid storage format metadata: expected 4 bytes, found {}",
            value.len(),
        ))
    })?;

    Ok(u32::from_be_bytes(bytes))
}

fn write_storage_format_version(tree: &Db, version: u32) -> Result<()> {
    tree.insert(STORAGE_FORMAT_VERSION_KEY, &version.to_be_bytes())
        .map_err(err_into)?;
    tree.flush().map_err(err_into)?;

    Ok(())
}

fn list_alive_table_names(tree: &Db) -> Result<Vec<String>> {
    let mut table_names = tree
        .scan_prefix(SCHEMA_PREFIX)
        .map(|item| {
            let (_, value) = item.map_err(err_into)?;
            let snapshot: Snapshot<Schema> = bincode::deserialize(&value).map_err(err_into)?;

            Ok(snapshot.get(u64::MAX, None).map(|schema| schema.table_name))
        })
        .filter_map(Result::transpose)
        .collect::<Result<Vec<_>>>()?;
    table_names.sort();

    Ok(table_names)
}
