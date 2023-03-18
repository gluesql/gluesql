use {
    crate::{concat_with, row, run, select, stringify_label, test_case},
    gluesql_core::prelude::{Payload, Value::Str},
};

test_case!(index, async move {
    let cases = vec![
        ("CREATE TABLE Meta (id INT, name TEXT)", Ok(Payload::Create)),
        (
            "CREATE INDEX Meta_id ON Meta (id)",
            Ok(Payload::CreateIndex),
        ),
        (
            "SELECT OBJECT_NAME, OBJECT_TYPE
                     FROM GLUE_OBJECTS",
            Ok(select!(
                OBJECT_NAME          | OBJECT_TYPE       ;
                Str                  | Str               ;
                "Meta_id".to_owned()   "INDEX".to_owned();
                "Meta".to_owned()      "TABLE".to_owned()
            )),
        ),
    ];

    for (actual, expected) in cases {
        test!(actual, expected);
    }
});
