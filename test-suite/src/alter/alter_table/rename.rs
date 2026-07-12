use super::*;

test_case!(rename, {
    let g = get_tester!();

    let test_cases = [
        (
            "CREATE TABLE Foo (id INTEGER, name TEXT);",
            Ok(Payload::Create),
        ),
        (
            "INSERT INTO Foo VALUES (1, 'a'), (2, 'b'), (3, 'c');",
            Ok(Payload::Insert(3)),
        ),
        ("SELECT id FROM Foo", Ok(select!(id; I64; 1; 2; 3))),
        (
            "ALTER TABLE Foo2 RENAME TO Bar;",
            Err(AlterTableError::TableNotFound("Foo2".to_owned()).into()),
        ),
        ("ALTER TABLE Foo RENAME TO Bar;", Ok(Payload::AlterTable)),
        ("SELECT id FROM Bar", Ok(select!(id; I64; 1; 2; 3))),
        (
            "ALTER TABLE Bar RENAME COLUMN id TO new_id",
            Ok(Payload::AlterTable),
        ),
        ("SELECT new_id FROM Bar", Ok(select!(new_id; I64; 1; 2; 3))),
        (
            "ALTER TABLE Bar RENAME COLUMN hello TO idid",
            Err(AlterTableError::RenamingColumnNotFound.into()),
        ),
        (
            // Cannot rename to duplicated column name
            "ALTER TABLE Bar RENAME COLUMN name TO new_id",
            Err(AlterTableError::AlreadyExistingColumn("new_id".to_owned()).into()),
        ),
    ];

    for (sql, expected) in test_cases {
        g.test(sql, expected);
    }
});
