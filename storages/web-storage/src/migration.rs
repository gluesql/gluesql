mod v1_to_v2;

use {
    crate::WebStorage,
    gluesql_core::error::{Error, Result},
};

pub const WEB_STORAGE_FORMAT_VERSION: u32 = 2;

const STORAGE_FORMAT_VERSION_PATH: &str = "gluesql-storage-format-version";

pub(super) fn ensure_storage_format_version_supported(storage: &WebStorage) -> Result<()> {
    match storage.get::<u32>(STORAGE_FORMAT_VERSION_PATH)? {
        Some(WEB_STORAGE_FORMAT_VERSION) => Ok(()),
        Some(version) if version > WEB_STORAGE_FORMAT_VERSION => Err(Error::StorageMsg(format!(
            "[WebStorage] unsupported newer format version v{version}"
        ))),
        Some(version) => Err(Error::StorageMsg(format!(
            "[WebStorage] unsupported format version v{version}"
        ))),
        None => {
            v1_to_v2::migrate(storage)?;
            write_latest_storage_format_version(storage)
        }
    }
}

fn write_latest_storage_format_version(storage: &WebStorage) -> Result<()> {
    storage.set(STORAGE_FORMAT_VERSION_PATH, WEB_STORAGE_FORMAT_VERSION)
}
