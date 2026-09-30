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

pub(crate) fn write_file_atomically(path: &Path, data: &str) -> Result<()> {
    let temp_path = temp_path_for(path);
    let backup_path = backup_path_for(path);
    let has_existing_target = path.exists();

    if let Err(err) = fs::File::create(&temp_path)
        .map_storage_err()
        .and_then(|mut file| {
            file.write_all(data.as_bytes()).map_storage_err()?;
            file.sync_all().map_storage_err()
        })
    {
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
