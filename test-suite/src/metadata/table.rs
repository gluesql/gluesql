use {
    crate::{row, run, select, stringify_label, test_case},
    gluesql_core::prelude::{Payload, Value::Str},
};

test_case!(table, async move {
    let cases = vec![
        ("CREATE TABLE Meta (id INT, name TEXT)", Ok(Payload::Create)),
        (
            "SELECT OBJECT_NAME, OBJECT_TYPE
                     FROM GLUE_OBJECTS
                     WHERE CREATED > NOW() - INTERVAL 1 MINUTE",
            Ok(select!(
                OBJECT_NAME       | OBJECT_TYPE       ;
                Str               | Str               ;
                "Meta".to_owned()   "TABLE".to_owned()
            )),
        ),
        ("DROP TABLE Meta", Ok(Payload::DropTable)),
        (
            "SELECT COUNT(*)
                     FROM GLUE_OBJECTS
                     WHERE CREATED > NOW() - INTERVAL 1 MINUTE",
            Ok(Payload::Select {
                labels: vec!["COUNT(*)".to_owned()],
                rows: Vec::new(),
            }),
        ),
    ];

    for (actual, expected) in cases {
        test!(actual, expected);
    }
});
