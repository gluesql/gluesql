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
        "SELECT id, name FROM Allegro ORDER BY id ASC",
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
        "SELECT id, name FROM Allegro ORDER BY id ASC",
        Ok(select!(
            id  | name
            I64 | Str;
            1     "hello".to_owned();
            2     "foo".to_owned();
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
        CREATE TABLE Allegro2 (
            table_id INTEGER,
            user_id INTEGER,
            other INTEGER,
            PRIMARY KEY (table_id, user_id)
        );
    ",
    )
    .await;

    // We attempt to insert a row in this table
    g.test(
        "INSERT INTO Allegro2 VALUES (1, 1, 7);",
        Ok(Payload::Insert(1)),
    )
    .await;

    // We check that the row was inserted correctly
    g.test(
        "SELECT table_id, user_id FROM Allegro2",
        Ok(select!(
            table_id | user_id
            I64     | I64;
            1       1
        )),
    )
    .await;

    // We attempt to insert a row with the same primary keys
    g.named_test(
        "Attempt to insert row with same primary keys",
        "INSERT INTO Allegro2 VALUES (1, 1, 89);",
        Err(
            ValidateError::DuplicateEntryOnPrimaryKeyField(Key::List(vec![
                Key::I64(1),
                Key::I64(1),
            ]))
            .into(),
        ),
    )
    .await;

    // We attempt to insert a row with a different primary key
    g.test(
        "INSERT INTO Allegro2 VALUES (1, 2, 37);",
        Ok(Payload::Insert(1)),
    )
    .await;

    // We check that the row was inserted correctly
    g.test(
        "SELECT table_id, user_id FROM Allegro2",
        Ok(select!(
            table_id | user_id
            I64     | I64;
            1       1;
            1       2
        )),
    )
    .await;

    // We check that the previous row was not deleted
    g.named_test(
        "Check row still exists",
        "SELECT table_id, user_id FROM Allegro2 WHERE table_id = 1;",
        Ok(select!(
            table_id | user_id
            I64     | I64;
            1       1;
            1       2
        )),
    )
    .await;

    // We query for the second component of the primary key
    g.named_test(
        "Check row still exists",
        "SELECT table_id, user_id FROM Allegro2 WHERE user_id = 2;",
        Ok(select!(
            table_id | user_id
            I64     | I64;
            1       2
        )),
    )
    .await;

    // We filter for both constraints at once
    g.named_test(
        "Check row still exists",
        "SELECT table_id, user_id FROM Allegro2 WHERE table_id = 1 AND user_id = 2;",
        Ok(select!(
            table_id | user_id
            I64     | I64;
            1       2
        )),
    )
    .await;

    // We try to delete a non-existing row
    g.named_test(
        "Attempt to delete non-existing row",
        "DELETE FROM Allegro2 WHERE table_id = 2 AND user_id = 2;",
        Ok(Payload::Delete(0)),
    )
    .await;

    // We delete the row we inserted
    g.named_test(
        "Delete row",
        "DELETE FROM Allegro2 WHERE table_id = 1 AND user_id = 2;",
        Ok(Payload::Delete(1)),
    )
    .await;

    // We check that the row was deleted
    g.named_test(
        "Check row was deleted",
        "SELECT table_id, user_id FROM Allegro2;",
        Ok(select!(
            table_id | user_id
            I64     | I64;
            1       1
        )),
    )
    .await;

    // We attempt to update a row with a different primary key
    g.named_test(
        "Attempt to update row with different primary key",
        "UPDATE Allegro2 SET table_id = 2 WHERE table_id = 1 AND user_id = 1;",
        Err(UpdateError::UpdateOnPrimaryKeyNotSupported("table_id".to_owned()).into()),
    )
    .await;

    // We check that the value associated to the primary key (1, 1) is equal to 7.
    g.named_test(
        "Check row value",
        "SELECT table_id, user_id, other FROM Allegro2 WHERE table_id = 1 AND user_id = 1;",
        Ok(select!(
            table_id | user_id | other
            I64     | I64    | I64;
            1       1       7
        )),
    )
    .await;

    // We update the value of the other column for the primary key (1, 1).
    g.named_test(
        "Update row",
        "UPDATE Allegro2 SET other = 3 WHERE table_id = 1 AND user_id = 1;",
        Ok(Payload::Update(1)),
    )
    .await;

    // We check that the updated value has been set correctly
    g.named_test(
        "Check row was updated",
        "SELECT table_id, user_id, other FROM Allegro2;",
        Ok(select!(
            table_id | user_id | other
            I64     | I64    | I64;
            1       1       3
        )),
    )
    .await;
});
