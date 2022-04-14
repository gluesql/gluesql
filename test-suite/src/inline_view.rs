/*
- [x] Add initial test cases at `test-suite/src/inline_view.rs`
- [] Fix UnsupportedQueryTableFactor at `core/src/translate/query.rs`
- [] Add TableFactor::Derived
-
*/

use {
    crate::*,
    gluesql_core::prelude::{Payload, Value::*},
};
test_case!(inline_view, async move {
    let test_cases = vec![
        (
            "CREATE TABLE Test (
                id INTEGER,
                name TEXT 
            )",
            Payload::Create,
        ),
        (
            "INSERT INTO Test VALUES (1, 'GLUE'), (2, 'SQL')",
            Payload::Insert(2),
        ),
        (
            "SELECT * FROM Test",
            select!(
                    id  | name
                    I64 | Str;
                    1     "GLUE".to_owned();
                    2     "SQL".to_owned()
            ),
        ),
        (
            "SELECT * FROM (SELECT COUNT(*) cnt FROM Test)",
            select!(id;I64;2),
        ),
    ];
    for (sql, expected) in test_cases {
        test!(Ok(expected), sql);
    }
});
