use {
    crate::error::StorageError,
    gluesql_core::{data::Key, error::Result},
};

pub(super) const SCHEMA_PREFIX: &[u8] = b"schema/";

fn validate_table_name(table_name: &str) -> Result<()> {
    if table_name.contains('/') {
        return Err(StorageError::TableNameContainsSlash(table_name.to_owned()).into());
    }

    Ok(())
}

pub(super) fn schema_key(table_name: &str) -> Result<Vec<u8>> {
    validate_table_name(table_name)?;
    Ok([SCHEMA_PREFIX, table_name.as_bytes()].concat())
}

pub(super) fn data_prefix(table_name: &str) -> Result<Vec<u8>> {
    validate_table_name(table_name)?;
    Ok([b"data/", table_name.as_bytes(), b"/"].concat())
}

pub(super) fn row_key(prefix: &[u8], key: &Key) -> Result<Vec<u8>> {
    let key = key.to_cmp_be_bytes()?;
    Ok([prefix, &key].concat())
}

#[cfg(test)]
mod tests {
    use gluesql_core::{
        data::{Key, KeyError},
        error::Error,
    };

    #[test]
    fn schema_key() {
        assert_eq!(super::schema_key("users"), Ok(b"schema/users".to_vec()));
        assert_eq!(
            super::schema_key("한글"),
            Ok("schema/한글".as_bytes().to_vec())
        );

        assert_eq!(
            super::schema_key("a/b"),
            Err(Error::StorageMsg(
                "table name cannot contain '/': a/b".into()
            ))
        );
    }

    #[test]
    fn data_prefix() {
        assert_eq!(super::data_prefix("users"), Ok(b"data/users/".to_vec()));
        assert_eq!(
            super::data_prefix("한글"),
            Ok("data/한글/".as_bytes().to_vec())
        );

        assert_eq!(
            super::data_prefix("a/b"),
            Err(Error::StorageMsg(
                "table name cannot contain '/': a/b".into()
            ))
        );
    }

    #[test]
    fn row_key() {
        let prefix = b"data/users/";

        // Core encodes string keys with a leading zero byte.
        assert_eq!(
            super::row_key(prefix, &Key::Str("a/b".into())),
            Ok(b"data/users/\0a/b".to_vec())
        );

        assert_eq!(
            super::row_key(prefix, &Key::F64(1.0.into())),
            Err(Error::Key(KeyError::FloatToCmpBigEndianNotSupported))
        );
    }
}
