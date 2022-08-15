use {
    crate::*,
    gluesql_core::{
        data::{Value::*, ValueError},
        executor::{UpdateError, ValidateError},
        prelude::Payload,
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
        Ok(Payload::Insert(2)),
        "INSERT INTO Allegro VALUES (1, 'hello'), (2, 'world');"
    );

    test!(
        Ok(select!(
            id  | name
            I64 | Str;
            1     "hello".to_owned();
            2     "world".to_owned()
        )),
        "SELECT id, name FROM Allegro"
    );
    test!(
        Ok(select!(
            id  | name
            I64 | Str;
            1     "hello".to_owned()
        )),
        "SELECT id, name FROM Allegro WHERE id = 1"
    );
    test!(
        Ok(select!(
            id  | name
            I64 | Str;
            1     "hello".to_owned()
        )),
        "SELECT id, name FROM Allegro WHERE id < 2"
    );
    test!(
        Ok(select!(id I64; 1; 2)),
        "
            SELECT a.id
            FROM Allegro a
            JOIN Allegro a2
            WHERE a.id = a2.id;
        "
    );
    test!(
        Ok(select!(id I64; 1; 2)),
        "
            SELECT id FROM Allegro WHERE id IN (
                SELECT id FROM Allegro WHERE id = id
            );
        "
    );

    run!("INSERT INTO Allegro VALUES (3, 'foo'), (4, 'bar'), (5, 'neon');");

    test!(
        Ok(select!(
            id  | name
            I64 | Str;
            1     "hello".to_owned();
            2     "world".to_owned();
            3     "foo".to_owned();
            4     "bar".to_owned();
            5     "neon".to_owned()
        )),
        "SELECT id, name FROM Allegro"
    );
    test!(
        Ok(select!(
            id  | name
            I64 | Str;
            2     "world".to_owned();
            4     "bar".to_owned()
        )),
        "SELECT id, name FROM Allegro WHERE id % 2 = 0"
    );
    test!(
        Ok(select!(
            id  | name
            I64 | Str;
            4     "bar".to_owned()
        )),
        "SELECT id, name FROM Allegro WHERE id = 4"
    );

    run!("DELETE FROM Allegro WHERE id > 3");
    test!(
        Ok(select!(
            id  | name
            I64 | Str;
            1     "hello".to_owned();
            2     "world".to_owned();
            3     "foo".to_owned()
        )),
        "SELECT id, name FROM Allegro"
    );

    // PRIMARY KEY includes UNIQUE constraint
    test!(
        Err(ValidateError::DuplicateEntryOnUniqueField(I64(1), "id".to_owned()).into()),
        "INSERT INTO Allegro VALUES (1, 'another hello');"
    );

    // PRIMARY KEY includes NOT NULL constraint
    test!(
        Err(ValueError::NullValueOnNotNullField.into()),
        "INSERT INTO Allegro VALUES (NULL, 'hello');"
    );

    // UPDATE is not allowed for PRIMARY KEY applied column
    test!(
        Err(UpdateError::UpdateOnPrimaryKeyNotSupported("id".to_owned()).into()),
        "UPDATE Allegro SET id = 100 WHERE id = 1"
    );
});
