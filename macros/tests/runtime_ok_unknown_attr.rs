use gluesql::{
    FromGlueRow,
    core::{data::Value, row_conversion::FromGlueRow as _},
};

#[derive(Debug, PartialEq, FromGlueRow)]
struct UnknownAttrIgnored {
    // Unknown key should be ignored by the macro (Ok(None) path)
    #[glue(not_rename = "ignored")]
    id: i64,
}

#[test]
fn unknown_glue_attribute_is_ignored() {
    let labels = vec!["id".to_string()];
    let row = vec![Value::I64(123)];
    let got = UnknownAttrIgnored::from_glue_row(&labels, &row).unwrap();
    assert_eq!(got, UnknownAttrIgnored { id: 123 });
}
