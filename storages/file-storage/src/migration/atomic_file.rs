use {
    crate::ResultExt,
    gluesql_core::error::{Error, Result},
    std::{
        ffi::OsStr,
        fs,
        io::Write,
        path::{Path, PathBuf},
    },
    uuid::Uuid,
};

const TEMP: &str = "tmp";
const BACKUP: &str = "bak";

/// Atomic for a single file and nothing more, which is why the whole-storage
/// migration uses a staging directory instead of a sequence of these.
pub(super) fn write(path: &Path, data: &str) -> Result<()> {
    let temp_path = suffixed(path, TEMP);
    let backup_path = suffixed(path, BACKUP);
    let has_existing_target = path.exists();

    let mut file = fs::File::create(&temp_path).map_storage_err()?;
    let written = file
        .write_all(data.as_bytes())
        .and_then(|()| file.sync_all())
        .map_storage_err();
    drop(file);

    if let Err(err) = written {
        let _ = fs::remove_file(&temp_path);
        return Err(err);
    }

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

pub(super) fn is_leftover(path: &Path) -> bool {
    path.extension()
        .and_then(OsStr::to_str)
        .and_then(|extension| extension.split_once('-'))
        .is_some_and(|(kind, _)| kind == TEMP || kind == BACKUP)
}

fn suffixed(path: &Path, kind: &str) -> PathBuf {
    let extension = path.extension().and_then(OsStr::to_str).unwrap_or_default();
    let suffix = Uuid::now_v7();

    path.with_extension(format!("{extension}.{kind}-{suffix}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn leftovers_are_recognised_by_their_extension() {
        assert!(is_leftover(Path::new(
            "Foo.sql.tmp-0192f000-0000-7000-8000-000000000000"
        )));
        assert!(is_leftover(Path::new(
            "0001.ron.bak-0192f000-0000-7000-8000-000000000000"
        )));
        assert!(!is_leftover(Path::new("Foo.sql")));
        assert!(!is_leftover(Path::new("0001.ron")));
        assert!(!is_leftover(Path::new("notes.txt")));
    }
}
