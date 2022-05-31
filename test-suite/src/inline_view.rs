/*
- [x] Add initial test cases at `test-suite/src/inline_view.rs`
- [x] Add `TableFactor::Derived {subquery, alias}` at `core/src/ast/query.rs`
- [x] Fix UnsupportedQueryTableFactor -> Return TableFactor::Derived at `core/src/translate/query.rs`
- [x] Should we separate TableFactor to TableFactorEvaluate and TableFactorTranslate?
    - adhoc Unreachable
- [ ] Impl if relation == Derived, select(subquery) in `select_with_label` at `core/src/executor/select/mod.rs`
- [ ] Sth to do in plan?
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
            "SELECT * FROM (SELECT COUNT(*) AS cnt FROM Test) AS InlineView",
            select!(id;I64;2),
        ),
    ];
    for (sql, expected) in test_cases {
        test!(Ok(expected), sql);
    }
});
