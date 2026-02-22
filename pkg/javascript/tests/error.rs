#![cfg(all(target_arch = "wasm32", not(feature = "nodejs")))]

wasm_bindgen_test_configure!(run_in_browser);

use {
    gloo_storage::{LocalStorage, SessionStorage, Storage},
    gloo_utils::format::JsValueSerdeExt,
    gluesql_js::Glue,
    serde_json::{Value as Json, json},
    wasm_bindgen::prelude::JsValue,
    wasm_bindgen_futures::JsFuture,
    wasm_bindgen_test::*,
};

fn clear_browser_storages() {
    LocalStorage::raw().clear().unwrap();
    SessionStorage::raw().clear().unwrap();
}

#[wasm_bindgen_test]
async fn error() {
    clear_browser_storages();

    let mut glue = Glue::new().unwrap();

    assert_eq!(
        glue.set_default_engine("something-else".to_owned()),
        Err(JsValue::from_str(
            "something-else is not supported (options: memory, localStorage, sessionStorage, indexedDB)",
        ))
    );

    assert_eq!(
        glue.set_default_engine("indexedDB".to_owned()),
        Err(JsValue::from_str(
            "indexedDB is not loaded - run loadIndexedDB() first",
        ))
    );

    let loaded = glue.load_indexeddb(None);
    JsFuture::from(loaded).await.unwrap();
    assert_eq!(glue.set_default_engine("indexedDB".to_owned()), Ok(()));

    let loaded = glue.load_indexeddb(None);
    assert_eq!(
        JsFuture::from(loaded).await,
        Err(JsValue::from_str("indexedDB storage is already loaded"))
    );

    let sql = "
        CREATE TABLE Mem (mid INTEGER) ENGINE = memory;
        CREATE TABLE Loc (lid INTEGER) ENGINE = localStorage;
        CREATE TABLE Ses (sid INTEGER) ENGINE = sessionStorage;
        CREATE TABLE Idb (iid INTEGER) ENGINE = indexedDB;
    ";
    let actual: Json = JsFuture::from(glue.query(sql.to_owned()))
        .await
        .unwrap()
        .into_serde()
        .unwrap();
    let expected = json!([
          { "type": "CREATE TABLE" },
          { "type": "CREATE TABLE" },
          { "type": "CREATE TABLE" },
          { "type": "CREATE TABLE" }
    ]);
    assert_eq!(actual, expected);

    clear_browser_storages();
}

#[wasm_bindgen_test]
async fn constructor_rejects_invalid_local_storage_format_version() {
    clear_browser_storages();
    LocalStorage::set("gluesql-storage-format-version", 999_u32).unwrap();

    let err = match Glue::new() {
        Ok(_) => panic!("constructor should fail"),
        Err(err) => err,
    };
    assert_eq!(
        err,
        JsValue::from_str(
            "[GlueSQL] failed to initialize localStorage engine: storage: [WebStorage] unsupported newer format version v999",
        )
    );

    clear_browser_storages();
}
