use {
    crate::*,
    gluesql_core::{
        error::InsertError,
        prelude::{Payload, Value::*},
    },
};

test_case!(array, async move {
    run!(
        "
CREATE TABLE Test (
    id INTEGER DEFAULT 1,
    surname TEXT NULL,
    name LIST NOT NULL,
);"
    );

    test! {
        name: "basic insert - single item",
        sql: "INSERT INTO Test (id, surname, name) VALUES (1, 'CHO', ['Seongbin','Bernie']);",
        expected: Ok(Payload::Insert(1))
    };
    test! {
        name: "insert multiple rows",
        sql: "
            INSERT INTO Test (id, surname, name)
            VALUES
            (3, 'CHO', Array['Seongbin','Bernie','Chobobdev']),
            (2, 'CHO', Array['devgony','Henry']);
        ",
        expected: Ok(Payload::Insert(2))
    };
    test! {
        sql: "INSERT INTO Test VALUES(5,'DOE', ['Jhon']);",
        expected: Ok(Payload::Insert(1))
    };
    
    test! {
        sql: "INSERT INTO Test (surname, name) VALUES ('DOE', ['Jane']);",
        expected: Ok(Payload::Insert(1))
    };

    test! {
        sql: "INSERT INTO Test (name) VALUES (['GlueSQL']);",
        expected: Ok(Payload::Insert(1))
    };

    test! {
        sql: "INSERT INTO Test (id, surname) VALUES (1, 'CHO');",
        expected: Err(InsertError::LackOfRequiredColumn("name".to_owned()).into())
    };

    test! {
        sql: "SELECT * FROM Test;",
        expected: Ok(select_with_null!(
            id     | surname                  | name;
            I64(1)   Str("CHO".to_owned())      List(vec![Str("Seongbin".to_owned()),Str("Bernie".to_owned())].to_owned());
            I64(3)   Str("CHO".to_owned())      List(vec![Str("Seongbin".to_owned()),Str("Bernie".to_owned()),Str("Chobobdev".to_owned())].to_owned());
            I64(2)   Str("CHO".to_owned())      List(vec![Str("devgony".to_owned()),Str("Henry".to_owned())].to_owned());
            I64(5)   Str("DOE".to_owned())      List(vec![Str("Jhon".to_owned())].to_owned());
            I64(1)   Str("DOE".to_owned())      List(vec![Str("Jane".to_owned())].to_owned());
            I64(1)   Null                       List(vec![Str("GlueSQL".to_owned())].to_owned())
        ))
    };

    run!("CREATE TABLE Target AS SELECT * FROM Test WHERE 1 = 0;");

    test! {
        name: "insert into target from source",
        sql: "INSERT INTO Target SELECT * FROM Test;",
        expected: Ok(Payload::Insert(6))
    };

    test! {
        name: "target rows are equivalent to source rows",
        sql: "SELECT * FROM Target;",
        expected: Ok(select_with_null!(
            id     | surname                  | name;
            I64(1)   Str("CHO".to_owned())      List(vec![Str("Seongbin".to_owned()),Str("Bernie".to_owned())].to_owned());
            I64(3)   Str("CHO".to_owned())      List(vec![Str("Seongbin".to_owned()),Str("Bernie".to_owned()),Str("Chobobdev".to_owned())].to_owned());
            I64(2)   Str("CHO".to_owned())      List(vec![Str("devgony".to_owned()),Str("Henry".to_owned())].to_owned());
            I64(5)   Str("DOE".to_owned())      List(vec![Str("Jhon".to_owned())].to_owned());
            I64(1)   Str("DOE".to_owned())      List(vec![Str("Jane".to_owned())].to_owned());
            I64(1)   Null                       List(vec![Str("GlueSQL".to_owned())].to_owned())
        ))
    };


    run!("DELETE FROM Item");
    run!("DELETE FROM Target");
});
