use {
    crate::*,
    gluesql_core::{
        data::{Value::*, ValueError},
        executor::{UpdateError, ValidateError},
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
        "INSERT INTO Allegro VALUES (1, 'hello'), (2, 'world');",
        Ok(Payload::Insert(2))
    );

    test!(
        "SELECT id, name FROM Allegro",
        Ok(select!(
            id  | name
            I64 | Str;
            1     "hello".to_owned();
            2     "world".to_owned()
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
        Ok(select!(id I64; 1; 2))
    );
    test!(
        "
            SELECT id FROM Allegro WHERE id IN (
                SELECT id FROM Allegro WHERE id = id
            );
        ",
        Ok(select!(id I64; 1; 2))
    );

    run!("INSERT INTO Allegro VALUES (3, 'foo'), (4, 'bar'), (5, 'neon');");

    test!(
        "SELECT id, name FROM Allegro",
        Ok(select!(
            id  | name
            I64 | Str;
            1     "hello".to_owned();
            2     "world".to_owned();
            3     "foo".to_owned();
            4     "bar".to_owned();
            5     "neon".to_owned()
        ))
    );
    test!(
        "SELECT id, name FROM Allegro WHERE id % 2 = 0",
        Ok(select!(
            id  | name
            I64 | Str;
            2     "world".to_owned();
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
            2     "world".to_owned();
            3     "foo".to_owned()
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
