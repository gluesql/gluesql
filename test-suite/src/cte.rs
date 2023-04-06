use {
    crate::*,
    gluesql_core::{
        prelude::{Payload, Value::*},
        translate::TranslateError,
    },
};
// failing test cases: recursive(not supported error), cte1 referencing cte2
// 1. cte 1개, 2. cte 1개 join 3. cte 2개 이상 4. cte2가 cte1을 가리킴.
//
test_case!(cte, async move {
    let test_cases = [
        (
            "CREATE TABLE InnerTable (
                id INTEGER,
                name TEXT 
            )",
            Ok(Payload::Create),
        ),
        (
            "CREATE TABLE OuterTable (
                id INTEGER,
                name TEXT 
            )",
            Ok(Payload::Create),
        ),
        (
            "INSERT INTO InnerTable VALUES (1, 'GLUE'), (2, 'SQL'), (3, 'SQL')",
            Ok(Payload::Insert(3)),
        ),
        (
            "INSERT INTO OuterTable VALUES (1, 'WORKS!'), (2, 'EXTRA')",
            Ok(Payload::Insert(2)),
        ),
        (
            "SELECT * FROM InnerTable",
            Ok(select!(
                    id  | name
                    I64 | Str;
                    1     "GLUE".to_owned();
                    2     "SQL".to_owned();
                    3     "SQL".to_owned()
            )),
        ),
        (
            "
            WITH
            Cte AS (SELECT COUNT(*) AS cnt FROM InnerTable)
            SELECT * FROM Cte",
            Ok(select!(cnt;I64;3)),
        ),
        (
            "
            WITH
            Cte AS (SELECT COUNT(*) AS cnt FROM InnerTable WHERE id > 1)
            SELECT * FROM Cte",
            Ok(select!(cnt;I64;2)),
        ),
        (
            "
            WITH 
            Cte AS (SELECT COUNT(*) FROM InnerTable) 
            SELECT * FROM Cte",
            Ok(select!("COUNT(*)";I64;3)),
        ),
        (
            // multiple cte
            "
            WITH
            Cte1 AS (SELECT COUNT(*) AS cnt FROM InnerTable),
            Cte2 AS (SELECT * FROM Cte1)
            SELECT * FROM Cte2",
            Ok(select!(cnt;I64;3)),
        ),
        (
            // join
            "
            WITH
            Cte1 AS (SELECT id, name FROM InnerTable)
            SELECT * FROM OuterTable JOIN Cte1 ON OuterTable.id = Cte1.id",
            Ok(select!(
                id  | name                | id  | name
                I64 | Str                 | I64 | Str;
                1     "WORKS!".to_owned()   1     "GLUE".to_owned();
                2     "EXTRA".to_owned()    2     "SQL".to_owned()
            )),
        ),
        (
            // join two cte
            "WITH
              Cte1 AS (SELECT id, name FROM InnerTable),
              Cte2 AS (SELECT id FROM InnerTable)
              SELECT * FROM Cte1 JOIN Cte2 ON Cte1.id = Cte2.id",
            Ok(select!(
                id  | name                | id
                I64 | Str                 | I64;
                1     "GLUE".to_owned()     1;
                2     "SQL".to_owned()      2;
                3     "SQL".to_owned()      3
            )),
        ),
        (
            // cte2 refers cte1, join two cte
            "WITH
             Cte1 AS (SELECT id, name FROM InnerTable LIMIT 2),
             Cte2 AS (SELECT id, name FROM Cte1)
             SELECT * FROM Cte1 JOIN Cte2 ON Cte1.name = Cte2.name",
            Ok(select!(
                id  | name                | id  | name
                I64 | Str                 | I64 | Str;
                1     "GLUE".to_owned()     1     "GLUE".to_owned();
                2     "SQL".to_owned()      2     "SQL".to_owned()
            )),
        ),
        (
            // three cte...
            "WITH
             Cte1 AS (SELECT id, name FROM InnerTable),
             Cte2 AS (SELECT * FROM Cte1),
             Cte3 AS (SELECT id FROM Cte2)
             SELECT * FROM Cte3",
            Ok(select!(id; I64; 1; 2; 3)),
        ),
        (
            // TODO Q. allow ?!? (mysql allows multiple with clause like these..)
            "WITH
             Cte2 AS (WITH Cte1 AS (SELECT * FROM InnerTable) SELECT id, name FROM Cte1)
             SELECT * FROM Cte2",
            Ok(select!(
                id  | name;
                I64 | Str;
                1     "GLUE".to_owned();
                2     "SQL".to_owned();
                3     "SQL".to_owned()
            )),
        ),
        (
            // recursive cte not supported
            "WITH RECURSIVE
             Cte1 AS (
                SELECT id FROM InnerTable
                UNION 
                SELECT id FROM Cte1 WHERE id < 2
             )
             SELECT * FROM Cte1",
            Err(TranslateError::UnsupportedRecursiveCte.into()),
        ),
    ];
    for (sql, expected) in test_cases {
        test!(sql, expected);
    }
});
