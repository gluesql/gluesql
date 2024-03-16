use {
    crate::*,
    gluesql_core::prelude::{Payload, Value::*},
};

test_case!(array, {
    let g = get_tester!();
    g.run("CREATE TABLE Test (id INTEGER DEFAULT 1,name LIST NOT NULL);")
        .await;

    g.named_test(
        "basic insert - single item",
        "INSERT INTO Test (id, name) VALUES (1, ['Seongbin','Bernie']);",
        Ok(Payload::Insert(1)),
    )
    .await;
    g.named_test("insert multiple rows","INSERT INTO Test (id, name) VALUES (3,Array['Seongbin','Bernie','Chobobdev']), (2,Array['devgony','Henry']);", Ok(Payload::Insert(2)),).await;
    g.test(
        "INSERT INTO Test VALUES(5,['Jhon']);",
        Ok(Payload::Insert(1)),
    )
    .await;
    g.test(
        "INSERT INTO Test (name) VALUES (['Jane']);",
        Ok(Payload::Insert(1)),
    )
    .await;
    g.test(
        "INSERT INTO Test (name) VALUES (['GlueSQL']);",
        Ok(Payload::Insert(1)),
    )
    .await;
    g.test("SELECT * FROM Test;",Ok(select_with_null!(
            id          | name;
            I64(1)        List(vec![Str("Seongbin".to_owned()),Str("Bernie".to_owned())]);
            I64(3)        List(vec![Str("Seongbin".to_owned()),Str("Bernie".to_owned()),Str("Chobobdev".to_owned())]);
            I64(2)        List(vec![Str("devgony".to_owned()),Str("Henry".to_owned())]);
            I64(5)        List(vec![Str("Jhon".to_owned())]);
            I64(1)        List(vec![Str("Jane".to_owned())]);
            I64(1)        List(vec![Str("GlueSQL".to_owned())])
        )),
    )

    .await;
    g.test(
        "SELECT ['name', 1, True] AS list;",
        Ok(Payload::Select {
            labels: vec!["list".to_owned()],
            rows: vec![vec![List(vec![Str("name".to_owned()), I64(1), Bool(true)])]],
        }),
    )
    .await;

    g.test(
        "SELECT ['GlueSQL', 1, True] [0] AS list;",
        Ok(Payload::Select {
            labels: vec!["list".to_owned()],
            rows: vec![vec![Str("GlueSQL".to_owned())]],
        }),
    )
    .await;
});
