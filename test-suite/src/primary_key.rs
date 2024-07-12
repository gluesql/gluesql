use {
    crate::*,
    gluesql_core::{
        data::Value::*,
        error::{UpdateError, ValidateError, ValueError},
        prelude::{Key, Payload},
    },
};

test_case!(primary_key, {
    let g = get_tester!();

    g.run(
        "
        CREATE TABLE Allegro (
            id INTEGER PRIMARY KEY,
            name TEXT,
        );
    ",
    )
    .await;
    g.test(
        "INSERT INTO Allegro VALUES (1, 'hello'), (3, 'world');",
        Ok(Payload::Insert(2)),
    )
    .await;

    g.test(
        "SELECT id, name FROM Allegro",
        Ok(select!(
            id  | name
            I64 | Str;
            1     "hello".to_owned();
            3     "world".to_owned()
        )),
    )
    .await;
    g.test(
        "SELECT id, name FROM Allegro WHERE id = 1",
        Ok(select!(
            id  | name
            I64 | Str;
            1     "hello".to_owned()
        )),
    )
    .await;
    g.test(
        "SELECT id, name FROM Allegro WHERE id < 2",
        Ok(select!(
            id  | name
            I64 | Str;
            1     "hello".to_owned()
        )),
    )
    .await;
    g.test(
        "
            SELECT a.id
            FROM Allegro a
            JOIN Allegro a2
            WHERE a.id = a2.id;
        ",
        Ok(select!(id I64; 1; 3)),
    )
    .await;
    g.test(
        "
            SELECT id FROM Allegro WHERE id IN (
                SELECT id FROM Allegro WHERE id = id
            );
        ",
        Ok(select!(id I64; 1; 3)),
    )
    .await;

    g.run("INSERT INTO Allegro VALUES (5, 'neon'), (2, 'foo'), (4, 'bar');")
        .await;

    g.test(
        "SELECT id, name FROM Allegro",
        Ok(select!(
            id  | name
            I64 | Str;
            1     "hello".to_owned();
            2     "foo".to_owned();
            3     "world".to_owned();
            4     "bar".to_owned();
            5     "neon".to_owned()
        )),
    )
    .await;
    g.test(
        "SELECT id, name FROM Allegro WHERE id % 2 = 0",
        Ok(select!(
            id  | name
            I64 | Str;
            2     "foo".to_owned();
            4     "bar".to_owned()
        )),
    )
    .await;
    g.test(
        "SELECT id, name FROM Allegro WHERE id = 4",
        Ok(select!(
            id  | name
            I64 | Str;
            4     "bar".to_owned()
        )),
    )
    .await;

    g.run("DELETE FROM Allegro WHERE id > 3").await;
    g.test(
        "SELECT id, name FROM Allegro",
        Ok(select!(
            id  | name
            I64 | Str;
            1     "hello".to_owned();
            2     "foo".to_owned();
            3     "world".to_owned()
        )),
    )
    .await;
    g.run(
        "
        CREATE TABLE Strslice (
            name TEXT PRIMARY KEY
        );
        ",
    )
    .await;
    g.run("INSERT INTO Strslice VALUES (SUBSTR(SUBSTR('foo', 1), 1));")
        .await;

    g.named_test(
        "PRIMARY KEY includes UNIQUE constraint",
        "INSERT INTO Allegro VALUES (1, 'another hello');",
        Err(ValidateError::DuplicateEntryOnPrimaryKeyField(Key::I64(1)).into()),
    )
    .await;

    g.named_test(
        "PRIMARY KEY includes NOT NULL constraint",
        "INSERT INTO Allegro VALUES (NULL, 'hello');",
        Err(ValueError::NullValueOnNotNullField.into()),
    )
    .await;

    g.named_test(
        "UPDATE is not allowed for PRIMARY KEY applied column",
        "UPDATE Allegro SET id = 100 WHERE id = 1",
        Err(UpdateError::UpdateOnPrimaryKeyNotSupported("id".to_owned()).into()),
    )
    .await;
});

test_case!(multiple_primary_keys, {
    let g = get_tester!();

    // We create a table with multiple primary keys.
    g.run(
        "
        CREATE TABLE Allegro (
            table_id INTEGER,
            user_id INTEGER,
            PRIMARY KEY (table_id, user_id)
        );
    ",
    )
    .await;

    // We attempt to insert a row in this table
    g.test("INSERT INTO Allegro VALUES (1, 1);", Ok(Payload::Insert(1)))
        .await;

    // We check that the row was inserted correctly
    g.test(
        "SELECT table_id, user_id FROM Allegro",
        Ok(select!(
            table_id | user_id
            I64     | I64;
            1       1
        )),
    )
    .await;

    // We attempt to insert a row with the same primary keys
    g.named_test(
        "Duplicate primary keys",
        "INSERT INTO Allegro VALUES (1, 1);",
        Err(
            ValidateError::DuplicateEntryOnPrimaryKeyField(vec![Key::I64(1), Key::I64(1)].into())
                .into(),
        ),
    )
    .await;

    // We attempt to insert a row with a different primary key
    g.test("INSERT INTO Allegro VALUES (1, 2);", Ok(Payload::Insert(1)))
        .await;

    // We check that the row was inserted correctly
    g.test(
        "SELECT table_id, user_id FROM Allegro",
        Ok(select!(
            table_id | user_id
            I64     | I64;
            1       1;
            1       2
        )),
    )
    .await;

    // We check that the previous row was not deleted
    // g.named_test(
    //     "Check previous row",
    //     "SELECT table_id, user_id FROM Allegro WHERE table_id = 1 AND user_id = 1;",
    //     Ok(select!(
    //         table_id | user_id
    //         I64     | I64;
    //         1       1
    //     )),
    // )
    // .await;
});
