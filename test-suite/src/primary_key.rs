use {
    crate::*,
    gluesql_core::{
        data::Value::*,
        error::{UpdateError, ValidateError, ValueError},
        prelude::{Key, Payload},
    },
};

test_case!(primary_key, async move {
    run!(
        "
        CREATE TABLE Allegro (
            id INTEGER PRIMARY KEY,
            name TEXT,
        );
    "
    );
    test!(
        "INSERT INTO Allegro VALUES (1, 'hello'), (3, 'world');",
        Ok(Payload::Insert(2))
    );

    test!(
        "SELECT id, name FROM Allegro",
        Ok(select!(
            id  | name
            I64 | Str;
            1     "hello".to_owned();
            3     "world".to_owned()
        ))
    );
    test!(
        "SELECT id, name FROM Allegro WHERE id = 1",
        Ok(select!(
            id  | name
            I64 | Str;
            1     "hello".to_owned()
        ))
    );
    test!(
        "SELECT id, name FROM Allegro WHERE id < 2",
        Ok(select!(
            id  | name
            I64 | Str;
            1     "hello".to_owned()
        ))
    );
    test!(
        "
            SELECT a.id
            FROM Allegro a
            JOIN Allegro a2
            WHERE a.id = a2.id;
        ",
        Ok(select!(id I64; 1; 3))
    );
    test!(
        "
            SELECT id FROM Allegro WHERE id IN (
                SELECT id FROM Allegro WHERE id = id
            );
        ",
        Ok(select!(id I64; 1; 3))
    );

    run!("INSERT INTO Allegro VALUES (5, 'neon'), (2, 'foo'), (4, 'bar');");

    test!(
        "SELECT id, name FROM Allegro",
        Ok(select!(
            id  | name
            I64 | Str;
            1     "hello".to_owned();
            2     "foo".to_owned();
            3     "world".to_owned();
            4     "bar".to_owned();
            5     "neon".to_owned()
        ))
    );
    test!(
        "SELECT id, name FROM Allegro WHERE id % 2 = 0",
        Ok(select!(
            id  | name
            I64 | Str;
            2     "foo".to_owned();
            4     "bar".to_owned()
        ))
    );
    test!(
        "SELECT id, name FROM Allegro WHERE id = 4",
        Ok(select!(
            id  | name
            I64 | Str;
            4     "bar".to_owned()
        ))
    );

    run!("DELETE FROM Allegro WHERE id > 3");
    test!(
        "SELECT id, name FROM Allegro",
        Ok(select!(
            id  | name
            I64 | Str;
            1     "hello".to_owned();
            2     "foo".to_owned();
            3     "world".to_owned()
        ))
    );
    run!(
        "
        CREATE TABLE Strslice (
            name TEXT PRIMARY KEY
        );
        "
    );
    run!("INSERT INTO Strslice VALUES (SUBSTR(SUBSTR('foo', 1), 1));");
    // PRIMARY KEY includes UNIQUE constraint
    test!(
        "INSERT INTO Allegro VALUES (1, 'another hello');",
        Err(ValidateError::DuplicateEntryOnPrimaryKeyField(Key::I64(1)).into())
    );

    // PRIMARY KEY includes NOT NULL constraint
    test!(
        "INSERT INTO Allegro VALUES (NULL, 'hello');",
        Err(ValueError::NullValueOnNotNullField.into())
    );

    // UPDATE is not allowed for PRIMARY KEY applied column
    test!(
        "UPDATE Allegro SET id = 100 WHERE id = 1",
        Err(UpdateError::UpdateOnPrimaryKeyNotSupported("id".to_owned()).into())
    );
});
