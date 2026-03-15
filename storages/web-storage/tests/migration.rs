#![cfg(target_arch = "wasm32")]

use {
    gloo_storage::{LocalStorage, SessionStorage, Storage},
    gluesql_core::{
        data::{Key, Value},
        error::Error,
    },
    gluesql_web_storage::{WEB_STORAGE_FORMAT_VERSION, WebStorage, WebStorageType},
    serde::Serialize,
    serde_json::json,
    std::collections::BTreeMap,
    wasm_bindgen_test::{wasm_bindgen_test, wasm_bindgen_test_configure},
};

wasm_bindgen_test_configure!(run_in_browser);

const STORAGE_FORMAT_VERSION_PATH: &str = "gluesql-storage-format-version";
const TABLE_NAMES_PATH: &str = "gluesql-schema-names";
const DATA_PATH: &str = "gluesql-data";

#[derive(Debug, Clone, Serialize)]
enum V1DataRow {
    Vec(Vec<Value>),
    Map(BTreeMap<String, Value>),
}

fn storage_types() -> [WebStorageType; 2] {
    [WebStorageType::Local, WebStorageType::Session]
}

fn storage_name(storage_type: WebStorageType) -> &'static str {
    match storage_type {
        WebStorageType::Local => "localStorage",
        WebStorageType::Session => "sessionStorage",
    }
}

fn clear_storage(storage_type: WebStorageType) {
    match storage_type {
        WebStorageType::Local => LocalStorage::raw().clear().unwrap(),
        WebStorageType::Session => SessionStorage::raw().clear().unwrap(),
    }
}

fn set_storage_item<T: Serialize>(storage_type: WebStorageType, key: &str, value: T) {
    match storage_type {
        WebStorageType::Local => LocalStorage::set(key, value).unwrap(),
        WebStorageType::Session => SessionStorage::set(key, value).unwrap(),
    }
}

#[wasm_bindgen_test]
async fn migrate_v1_rows_to_v2_on_new() {
    for storage_type in storage_types() {
        clear_storage(storage_type);
        set_storage_item(storage_type, TABLE_NAMES_PATH, vec!["Foo".to_owned()]);
        set_storage_item(
            storage_type,
            "gluesql-data/Foo",
            vec![
                (Key::I64(1), V1DataRow::Vec(vec![Value::I64(1)])),
                (
                    Key::I64(2),
                    V1DataRow::Map(BTreeMap::from([("id".to_owned(), Value::I64(2))])),
                ),
            ],
        );

        let storage = WebStorage::new(storage_type).unwrap();

        assert_eq!(
            storage.get::<u32>(STORAGE_FORMAT_VERSION_PATH).unwrap(),
            Some(WEB_STORAGE_FORMAT_VERSION),
            "{}",
            storage_name(storage_type)
        );
        assert_eq!(
            storage
                .get::<Vec<(Key, Vec<Value>)>>(&format!("{DATA_PATH}/Foo"))
                .unwrap(),
            Some(vec![
                (Key::I64(1), vec![Value::I64(1)]),
                (
                    Key::I64(2),
                    vec![Value::Map(BTreeMap::from([(
                        "id".to_owned(),
                        Value::I64(2)
                    )]))]
                ),
            ]),
            "{}",
            storage_name(storage_type)
        );

        clear_storage(storage_type);
    }
}

#[wasm_bindgen_test]
async fn initialize_empty_storage_to_v2() {
    for storage_type in storage_types() {
        clear_storage(storage_type);

        let storage = WebStorage::new(storage_type).unwrap();

        assert_eq!(
            storage.get::<u32>(STORAGE_FORMAT_VERSION_PATH).unwrap(),
            Some(WEB_STORAGE_FORMAT_VERSION),
            "{}",
            storage_name(storage_type)
        );

        clear_storage(storage_type);
    }
}

#[wasm_bindgen_test]
async fn initialize_when_non_gluesql_keys_exist() {
    for storage_type in storage_types() {
        clear_storage(storage_type);
        set_storage_item(storage_type, "custom-key", "custom-value");

        let storage = WebStorage::new(storage_type).unwrap();

        assert_eq!(
            storage.get::<u32>(STORAGE_FORMAT_VERSION_PATH).unwrap(),
            Some(WEB_STORAGE_FORMAT_VERSION),
            "{}",
            storage_name(storage_type)
        );

        clear_storage(storage_type);
    }
}

#[wasm_bindgen_test]
async fn reject_unsupported_versions() {
    for storage_type in storage_types() {
        clear_storage(storage_type);
        set_storage_item(
            storage_type,
            STORAGE_FORMAT_VERSION_PATH,
            WEB_STORAGE_FORMAT_VERSION + 1,
        );

        let err = match WebStorage::new(storage_type) {
            Ok(_) => panic!("{} should fail", storage_name(storage_type)),
            Err(err) => err,
        };
        assert_eq!(
            err,
            Error::StorageMsg(format!(
                "[WebStorage] unsupported newer format version v{}",
                WEB_STORAGE_FORMAT_VERSION + 1
            )),
            "{}",
            storage_name(storage_type)
        );

        clear_storage(storage_type);
        set_storage_item(storage_type, STORAGE_FORMAT_VERSION_PATH, 0_u32);

        let err = match WebStorage::new(storage_type) {
            Ok(_) => panic!("{} should fail", storage_name(storage_type)),
            Err(err) => err,
        };
        assert_eq!(
            err,
            Error::StorageMsg("[WebStorage] unsupported format version v0".to_owned()),
            "{}",
            storage_name(storage_type)
        );

        clear_storage(storage_type);
    }
}

#[wasm_bindgen_test]
async fn reject_invalid_v1_payload() {
    for storage_type in storage_types() {
        clear_storage(storage_type);
        set_storage_item(storage_type, TABLE_NAMES_PATH, vec!["Foo".to_owned()]);
        set_storage_item(storage_type, "gluesql-data/Foo", json!([1, 2, 3]));

        let err = match WebStorage::new(storage_type) {
            Ok(_) => panic!("{} should fail", storage_name(storage_type)),
            Err(err) => err,
        };
        assert_eq!(
            err,
            Error::StorageMsg(
                "[WebStorage] conflict - failed to parse v1 row payload in table 'Foo'".to_owned()
            ),
            "{}",
            storage_name(storage_type)
        );

        clear_storage(storage_type);
    }
}
