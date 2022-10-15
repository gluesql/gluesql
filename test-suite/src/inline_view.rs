use {
    crate::*,
    gluesql_core::{
        executor::EvaluateError,
        prelude::{Payload, Value::*},
        translate::TranslateError,
    },
};
test_case!(inline_view, async move {
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
            "SELECT *
            FROM (
                SELECT COUNT(*) AS cnt FROM InnerTable
            ) AS InlineView",
            Ok(select!(cnt;I64;3)),
        ),
        (
            // inline view with WHERE clause
            "SELECT *
            FROM (
                SELECT COUNT(*) AS cnt
                FROM InnerTable
                WHERE id > 1
            ) AS InlineView",
            Ok(select!(cnt;I64;2)),
        ),
        (
            // inline view without column alias
            "SELECT *
            FROM (
                SELECT COUNT(*) FROM InnerTable
            ) AS InlineView",
            Ok(select!("COUNT(*)";I64;3)),
        ),
        (
            // cannot use inline view without table alias
            "SELECT *
            FROM (
                SELECT COUNT(*) AS cnt FROM InnerTable
            )",
            Err(TranslateError::LackOfAlias.into()),
        ),
        (
            // inline view more than twice
            "SELECT *
            FROM (
                SELECT *
                FROM (
                    SELECT COUNT(*) AS cnt FROM InnerTable
                ) AS InlineView
            ) AS InlineView2",
            Ok(select!(cnt;I64;3)),
        ),
        (
            // join - Expr
            "SELECT *
            FROM OuterTable
            JOIN (
                SELECT id, name FROM InnerTable
            ) AS InlineView ON OuterTable.id = InlineView.id",
            Ok(select!(
                id  | name                | id  | name
                I64 | Str                 | I64 | Str;
                1     "WORKS!".to_owned()   1     "GLUE".to_owned();
                2     "EXTRA".to_owned()    2     "SQL".to_owned()
            )),
        ),
        (
            // join - Expr should include join column
            "SELECT *
            FROM OuterTable JOIN (
                SELECT name FROM InnerTable
            ) AS InlineView ON OuterTable.id = InlineView.id",
            Err(EvaluateError::ValueNotFound("id".to_owned()).into()),
        ),
        (
            // join - Expr with WHERE clause
            "SELECT *
            FROM OuterTable
            JOIN (
                SELECT id, name
                FROM InnerTable
                WHERE id = 1 
            ) AS InlineView ON OuterTable.id = InlineView.id",
            Ok(select!(
                id  | name                | id  | name
                I64 | Str                 | I64 | Str;
                1     "WORKS!".to_owned()   1     "GLUE".to_owned()
            )),
        ),
        (
            // join - Wildcard
            "SELECT *
            FROM OuterTable JOIN (
                SELECT * FROM InnerTable
            ) AS InlineView ON OuterTable.id = InlineView.id",
            Ok(select!(
                id  | name                | id  | name
                I64 | Str                 | I64 | Str;
                1     "WORKS!".to_owned()   1     "GLUE".to_owned();
                2     "EXTRA".to_owned()    2     "SQL".to_owned()
            )),
        ),
        (
            // join - QualifiedWildcard at inner projection
            "SELECT * 
            FROM OuterTable JOIN (
                SELECT InnerTable.* FROM InnerTable
            ) AS InlineView ON OuterTable.id = InlineView.id",
            Ok(select!(
                id  | name                | id  | name
                I64 | Str                 | I64 | Str;
                1     "WORKS!".to_owned()   1     "GLUE".to_owned();
                2     "EXTRA".to_owned()    2     "SQL".to_owned()
            )),
        ),
        (
            // join - QualifiedWildcard at outer projection
            "SELECT InlineView.*
            FROM OuterTable JOIN (
                SELECT InnerTable.*, 'once' AS literal FROM InnerTable
            ) AS InlineView ON OuterTable.id = InlineView.id",
            Ok(select!(
                id  | name               | literal
                I64 | Str                | Str;
                1     "GLUE".to_owned()    "once".to_owned();
                2     "SQL".to_owned()     "once".to_owned()
            )),
        ),
        (
            // join - inline view more than twice
            "SELECT * 
            FROM OuterTable
            JOIN (
                SELECT OuterTable.id, OuterTable.name 
                FROM OuterTable 
                JOIN (
                    SELECT * FROM InnerTable
                ) AS InlineView ON OuterTable.id = InlineView.id
            ) AS InlineView2 ON OuterTable.id = InlineView2.id",
            Ok(select!(
                id  | name                | id  | name
                I64 | Str                 | I64 | Str;
                1     "WORKS!".to_owned()   1     "WORKS!".to_owned();
                2     "EXTRA".to_owned()   2     "EXTRA".to_owned()
            )),
        ),
        (
            // group by
            "SELECT *
            FROM (
                SELECT name, count(*) as cnt
                FROM InnerTable
                GROUP BY name
             ) AS InlineView",
            Ok(select!(
                name             | cnt
                Str              | I64;
                "GLUE".to_owned()  1;
                "SQL".to_owned()   2
            )),
        ),
        (
            // limit
            "SELECT * FROM (
                SELECT *
                FROM InnerTable
                LIMIT 1
             ) AS InlineView",
            Ok(select!(
                id  | name
                I64 | Str;
                1    "GLUE".to_owned()
            )),
        ),
        (
            // offset
            "SELECT * FROM (
                SELECT *
                FROM InnerTable
                OFFSET 2
             ) AS InlineView",
            Ok(select!(
                id  | name
                I64 | Str;
                3    "SQL".to_owned()
            )),
        ),
        (
            // order by: can return error by different plan in the future
            "SELECT * FROM (
                SELECT *
                FROM InnerTable
                ORDER BY id desc
             ) AS InlineView",
            Ok(select!(
                id  | name
                I64 | Str;
                3    "SQL".to_owned();
                2    "SQL".to_owned();
                1    "GLUE".to_owned()
            )),
        ),
        (
            // unsupported implicit join
            "SELECT *
            FROM OuterTable, (
                    SELECT id
                    FROM InnerTable
                    WHERE InnerTable.id = OuterTable.id
                ) AS InlineView",
            Err(TranslateError::TooManyTables.into()),
        ),
        (
            // unsupported select distinct
            "SELECT DISTINCT id FROM OuterTable",
            Err(TranslateError::SelectDistinctNotSupported.into()),
        ),
        (
            // inline view subquery + join with inline view
            "SELECT *
            FROM (
                SELECT *
                FROM InnerTable 
            ) AS InlineView
            Join OuterTable ON InlineView.id = OuterTable.id",
            Ok(select!(
                id  | name                | id  | name
                I64 | Str                 | I64 | Str;
                1     "GLUE".to_owned()   1     "WORKS!".to_owned();
                2     "SQL".to_owned()    2     "EXTRA".to_owned()
            )),
        ),
    ];
    for (sql, expected) in test_cases {
        test!(sql, expected);
    }
});
