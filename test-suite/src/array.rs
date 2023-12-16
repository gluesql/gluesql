use {
    crate::*,
    gluesql_core::{
        error::InsertError,
        prelude::{Payload, Value::*},
    },
};

test_case!(array, {
    let g = get_tester!();
    g.run("CREATE TABLE Test (id INTEGER DEFAULT 1,surname TEXT NULL, name LIST NOT NULL);")
        .await;

    g.named_test(
        "basic insert - single item",
        "INSERT INTO Test (id, surname, name) VALUES (1, 'CHO', ['Seongbin','Bernie']);",
        Ok(Payload::Insert(1)),
    )
    .await;
    g.named_test("insert multiple rows","INSERT INTO Test (id, surname, name) VALUES (3, 'CHO', Array['Seongbin','Bernie','Chobobdev']), (2, 'CHO', Array['devgony','Henry']);", Ok(Payload::Insert(2)),).await;
    g.test(
        "INSERT INTO Test VALUES(5,'DOE', ['Jhon']);",
        Ok(Payload::Insert(1)),
    )
    .await;
    g.test(
        "INSERT INTO Test (surname, name) VALUES ('DOE', ['Jane']);",
        Ok(Payload::Insert(1)),
    )
    .await;
    g.test(
        "INSERT INTO Test (name) VALUES (['GlueSQL']);",
        Ok(Payload::Insert(1)),
    )
    .await;
    g.test(
        "INSERT INTO Test (id, surname) VALUES (1, 'CHO');",
        Err(InsertError::LackOfRequiredColumn("name".to_owned()).into()),
    )
    .await;
    g.test("SELECT * FROM Test;",Ok(select_with_null!(
            id     | surname                  | name;
            I64(1)   Str("CHO".to_owned())      List(vec![Str("Seongbin".to_owned()),Str("Bernie".to_owned())]);
            I64(3)   Str("CHO".to_owned())      List(vec![Str("Seongbin".to_owned()),Str("Bernie".to_owned()),Str("Chobobdev".to_owned())]);
            I64(2)   Str("CHO".to_owned())      List(vec![Str("devgony".to_owned()),Str("Henry".to_owned())]);
            I64(5)   Str("DOE".to_owned())      List(vec![Str("Jhon".to_owned())]);
            I64(1)   Str("DOE".to_owned())      List(vec![Str("Jane".to_owned())]);
            I64(1)   Null                       List(vec![Str("GlueSQL".to_owned())])
        )),
    ).await;

    g.run("CREATE TABLE Target AS SELECT * FROM Test WHERE 1 = 0;")
        .await;

    g.named_test(
        "insert into target from source",
        "INSERT INTO Target SELECT * FROM Test;",
        Ok(Payload::Insert(6)),
    )
    .await;

    g.named_test("target rows are equivalent to source rows","SELECT * FROM Target;",Ok(select_with_null!(
            id     | surname                  | name;
            I64(1)   Str("CHO".to_owned())      List(vec![Str("Seongbin".to_owned()),Str("Bernie".to_owned())]);
            I64(3)   Str("CHO".to_owned())      List(vec![Str("Seongbin".to_owned()),Str("Bernie".to_owned()),Str("Chobobdev".to_owned())]);
            I64(2)   Str("CHO".to_owned())      List(vec![Str("devgony".to_owned()),Str("Henry".to_owned())]);
            I64(5)   Str("DOE".to_owned())      List(vec![Str("Jhon".to_owned())]);
            I64(1)   Str("DOE".to_owned())      List(vec![Str("Jane".to_owned())]);
            I64(1)   Null                       List(vec![Str("GlueSQL".to_owned())])
        )),
    ).await;

    g.run("DELETE FROM Item").await;
    g.run("DELETE FROM Target").await;
});
