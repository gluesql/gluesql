#![cfg(all(target_arch = "wasm32", not(feature = "nodejs")))]

wasm_bindgen_test_configure!(run_in_browser);

use {
    gloo_utils::format::JsValueSerdeExt,
    gluesql_js::Glue,
    serde_json::{json, Value as Json},
    wasm_bindgen::prelude::JsValue,
    wasm_bindgen_futures::JsFuture,
    wasm_bindgen_test::*,
};

#[wasm_bindgen_test]
async fn error() {
    let mut glue = Glue::new();

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

    let loaded = glue.load_indexeddb();
    JsFuture::from(loaded).await.unwrap();
    assert_eq!(glue.set_default_engine("indexedDB".to_owned()), Ok(()));

    let loaded = glue.load_indexeddb();
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
}
