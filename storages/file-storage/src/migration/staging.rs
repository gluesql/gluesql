use {
    super::{atomic_file, paths, schema_file},
    crate::{FileStorage, ResultExt},
    gluesql_core::{
        data::{Key, Schema, Value},
        error::{Error, Result},
        store::StoreMut,
    },
    std::{
        collections::BTreeSet,
        ffi::OsStr,
        fs,
        path::{Path, PathBuf},
    },
};

const SCHEMA_EXTENSION: &str = "sql";
const ROW_EXTENSION: &str = "ron";

pub(super) struct SourceRow {
    pub(super) key: Key,
    pub(super) values: Vec<Value>,
    pub(super) converted: bool,
}

/// Injected so this module stays free of any one format step.
pub(super) type DecodeRow = fn(&Path) -> Result<SourceRow>;

pub(super) fn build(source: &Path, staging: &Path, decode: DecodeRow) -> Result<usize> {
    let schemas = read_schemas(source)?;
    let table_names = schemas
        .iter()
        .map(|(table_name, _)| table_name.clone())
        .collect::<BTreeSet<_>>();

    let mut build = Build {
        source,
        staging,
        target: FileStorage::new(staging)?,
        decode,
    };
    let mut rewritten_rows = 0;

    for (table_name, schema) in &schemas {
        build.target.insert_schema(schema)?;
        rewritten_rows += build.copy_table(table_name)?;
    }

    copy_foreign_entries(source, staging, &table_names)?;
    mirror_dir_modes(source, staging)?;
    sync_tree(staging)?;

    Ok(rewritten_rows)
}

/// Each schema is staged under the name its DDL declares, not its file name.
fn read_schemas(source: &Path) -> Result<Vec<(String, Schema)>> {
    schema_file::list_paths(source)?
        .into_iter()
        .map(|schema_path| {
            let table_name = schema_file::table_name(&schema_path)?;
            let schema = Schema::from_ddl(&schema_file::read(&schema_path)?.ddl)?;

            if schema.table_name != table_name {
                return Err(Error::StorageMsg(format!(
                    "[FileStorage] '{}' declares the table '{}', which its file name does not match; migrating it would overwrite that table and drop this file, so move it out of the storage directory first, no data was modified",
                    schema_path.display(),
                    schema.table_name
                )));
            }

            Ok((table_name, schema))
        })
        .collect()
}

struct Build<'a> {
    source: &'a Path,
    staging: &'a Path,
    target: FileStorage,
    decode: DecodeRow,
}

impl Build<'_> {
    fn copy_table(&mut self, table_name: &str) -> Result<usize> {
        let table_path = self.source.join(table_name);
        if !table_path.exists() {
            return Ok(0);
        }

        let entries = scan_table_dir(&table_path)?;
        let mut rewritten_rows = 0;

        for row_path in entries.rows {
            let row = (self.decode)(&row_path)?;
            let staged = self.target.data_path(table_name, &row.key)?;

            if staged.file_name() != row_path.file_name() {
                return Err(Error::StorageMsg(format!(
                    "[FileStorage] the row in '{}' holds a key that does not name its file; staging derives the name from the key, so this row would take the place of '{}'. Resolve the mismatch before migrating, no data was modified",
                    row_path.display(),
                    staged.display()
                )));
            }

            self.target
                .insert_data(table_name, vec![(row.key, row.values)])?;

            if row.converted {
                rewritten_rows += 1;
            }
        }

        let staged_table = self.staging.join(table_name);
        for ForeignEntry { path, file_type } in entries.foreign {
            let name = path.file_name().unwrap_or_default();

            copy_entry(&path, &staged_table.join(name), file_type)?;
        }

        Ok(rewritten_rows)
    }
}

pub(super) fn reject_unmigratable_entries(source: &Path) -> Result<()> {
    let table_names = schema_file::list_paths(source)?
        .iter()
        .map(|schema_path| schema_file::table_name(schema_path))
        .collect::<Result<BTreeSet<_>>>()?;

    reject_entries_under(source, &table_names, SCHEMA_EXTENSION)
}

#[derive(Debug)]
struct ForeignEntry {
    path: PathBuf,
    file_type: fs::FileType,
}

#[derive(Debug, Default)]
struct TableEntries {
    rows: Vec<PathBuf>,
    foreign: Vec<ForeignEntry>,
}

fn scan_table_dir(table_path: &Path) -> Result<TableEntries> {
    let mut entries = TableEntries::default();

    for entry in fs::read_dir(table_path).map_storage_err()? {
        let entry = entry.map_storage_err()?;
        let file_type = entry.file_type().map_storage_err()?;
        let path = entry.path();
        let is_row =
            file_type.is_file() && path.extension().and_then(OsStr::to_str) == Some(ROW_EXTENSION);

        if is_row {
            entries.rows.push(path);
        } else {
            entries.foreign.push(ForeignEntry { path, file_type });
        }
    }

    entries.rows.sort();
    entries.foreign.sort_by(|a, b| a.path.cmp(&b.path));

    Ok(entries)
}

fn copy_foreign_entries(
    source: &Path,
    target: &Path,
    table_names: &BTreeSet<String>,
) -> Result<()> {
    for entry in fs::read_dir(source).map_storage_err()? {
        let entry = entry.map_storage_err()?;
        let path = entry.path();
        let file_type = entry.file_type().map_storage_err()?;
        let is_table_dir = file_type.is_dir()
            && path
                .file_name()
                .and_then(OsStr::to_str)
                .is_some_and(|name| table_names.contains(name));
        let is_migrated_file = file_type.is_file()
            && path.extension().and_then(OsStr::to_str) == Some(SCHEMA_EXTENSION);

        if is_table_dir || is_migrated_file {
            continue;
        }

        copy_entry(&path, &target.join(entry.file_name()), file_type)?;
    }

    Ok(())
}

fn copy_entry(source: &Path, target: &Path, file_type: fs::FileType) -> Result<()> {
    if file_type.is_symlink() {
        return copy_symlink(source, target);
    }

    if file_type.is_file() {
        fs::copy(source, target).map_storage_err()?;

        return Ok(());
    }

    if !file_type.is_dir() {
        return Err(Error::StorageMsg(format!(
            "[FileStorage] cannot copy '{}' into the migrated storage: it is neither a file nor a directory",
            source.display()
        )));
    }

    fs::create_dir_all(target).map_storage_err()?;
    for entry in fs::read_dir(source).map_storage_err()? {
        let entry = entry.map_storage_err()?;
        let file_type = entry.file_type().map_storage_err()?;

        copy_entry(&entry.path(), &target.join(entry.file_name()), file_type)?;
    }

    Ok(())
}

#[cfg(unix)]
fn copy_symlink(source: &Path, target: &Path) -> Result<()> {
    let link = fs::read_link(source).map_storage_err()?;

    std::os::unix::fs::symlink(link, target).map_storage_err()
}

#[cfg(not(unix))]
fn copy_symlink(source: &Path, _target: &Path) -> Result<()> {
    Err(Error::StorageMsg(format!(
        "[FileStorage] cannot copy the symbolic link '{}' into the migrated storage on this platform; remove it before migrating, no data was modified",
        source.display()
    )))
}

/// `StoreMut::insert_data` does not fsync, and cutover removes the backup.
/// Directories are flushed after their entries, so no published row is nameless.
fn sync_tree(path: &Path) -> Result<()> {
    for entry in fs::read_dir(path).map_storage_err()? {
        let entry = entry.map_storage_err()?;
        let file_type = entry.file_type().map_storage_err()?;
        let path = entry.path();

        if file_type.is_dir() {
            sync_tree(&path)?;
            continue;
        }

        if file_type.is_symlink() {
            continue;
        }

        fs::File::open(&path)
            .map_storage_err()?
            .sync_all()
            .map_storage_err()?;
    }

    paths::sync_dir(path)
}

/// `create_dir_all` gives staged directories the default mode, not the source's.
#[cfg(unix)]
fn mirror_dir_modes(source: &Path, staging: &Path) -> Result<()> {
    for entry in fs::read_dir(staging).map_storage_err()? {
        let entry = entry.map_storage_err()?;

        if entry.file_type().map_storage_err()?.is_dir() {
            mirror_dir_modes(&source.join(entry.file_name()), &entry.path())?;
        }
    }

    match fs::symlink_metadata(source) {
        Ok(metadata) if metadata.is_dir() => {
            fs::set_permissions(staging, metadata.permissions()).map_storage_err()
        }
        _ => Ok(()),
    }
}

#[cfg(not(unix))]
fn mirror_dir_modes(_source: &Path, _staging: &Path) -> Result<()> {
    Ok(())
}

fn reject_entries_under(
    dir: &Path,
    table_names: &BTreeSet<String>,
    data_extension: &str,
) -> Result<()> {
    for entry in fs::read_dir(dir).map_storage_err()? {
        let entry = entry.map_storage_err()?;
        let path = entry.path();
        let file_type = entry.file_type().map_storage_err()?;
        let name = path.file_name().and_then(OsStr::to_str);

        if file_type.is_symlink()
            && (path.extension().and_then(OsStr::to_str) == Some(data_extension)
                || name.is_some_and(|name| table_names.contains(name)))
        {
            return Err(Error::StorageMsg(format!(
                "[FileStorage] '{}' is a symbolic link where the storage keeps data; the migration would stage the link itself rather than what it points at, so replace it before migrating, no data was modified",
                path.display()
            )));
        }

        if file_type.is_dir() && name.is_some_and(|name| table_names.contains(name)) {
            reject_entries_under(&path, &BTreeSet::new(), ROW_EXTENSION)?;
            continue;
        }

        if atomic_file::is_leftover(&path) {
            return Err(Error::StorageMsg(format!(
                "[FileStorage] unexpected entry '{}' left by an interrupted write; a '.bak-' entry holds the previous contents and may be the only copy of the file it replaces, so restore it under that name before migrating, no data was modified",
                path.display()
            )));
        }
    }

    Ok(())
}
