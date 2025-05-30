use {
    crate::*,
    Value::*,
    gluesql_core::{ast_builder::*, prelude::*},
};

test_case!(ast_builder, {
    let glue = get_glue!();

    let actual = table("TxTest")
        .create_table()
        .add_column("id INTEGER")
        .add_column("name TEXT")
        .execute(glue)
        .await;
    let expected = Ok(Payload::Create);
    assert_eq!(actual, expected);

    let actual = table("TxTest")
        .insert()
        .columns("id, name")
        .values(vec![
            vec![num(1), text("Friday")],
            vec![num(2), text("Phone")],
        ])
        .execute(glue)
        .await;
    let expected = Ok(Payload::Insert(2));
    assert_eq!(actual, expected);

    let actual = begin().execute(glue).await;
    let expected = Ok(Payload::StartTransaction);
    assert_eq!(actual, expected);

    let actual = table("TxTest")
        .insert()
        .columns("id, name")
        .values(vec![vec![num(3), text("Vienna")]])
        .execute(glue)
        .await;
    let expected = Ok(Payload::Insert(1));
    assert_eq!(actual, expected);

    let actual = rollback().execute(glue).await;
    let expected = Ok(Payload::Rollback);
    assert_eq!(actual, expected);

    let actual = table("TxTest")
        .select()
        .project("id, name")
        .execute(glue)
        .await;
    let expected = Ok(select!(
        id  | name
        I64 | Str;
        1     "Friday".to_owned();
        2     "Phone".to_owned()
    ));
    assert_eq!(actual, expected);

    let actual = begin().execute(glue).await;
    let expected = Ok(Payload::StartTransaction);
    assert_eq!(actual, expected);

    let actual = table("TxTest")
        .insert()
        .columns("id, name")
        .values(vec![vec![num(3), text("Vienna")]])
        .execute(glue)
        .await;
    let expected = Ok(Payload::Insert(1));
    assert_eq!(actual, expected);

    let actual = table("TxTest")
        .select()
        .project("id, name")
        .execute(glue)
        .await;
    let expected = Ok(select!(
        id  | name
        I64 | Str;
        1     "Friday".to_owned();
        2     "Phone".to_owned();
        3     "Vienna".to_owned()
    ));
    assert_eq!(actual, expected);

    let actual = commit().execute(glue).await;
    let expected = Ok(Payload::Commit);
    assert_eq!(actual, expected);

    let actual = table("TxTest")
        .select()
        .project("id, name")
        .execute(glue)
        .await;
    let expected = Ok(select!(
        id  | name
        I64 | Str;
        1     "Friday".to_owned();
        2     "Phone".to_owned();
        3     "Vienna".to_owned()
    ));
    assert_eq!(actual, expected);

    // DELETE
    let actual = begin().execute(glue).await;
    let expected = Ok(Payload::StartTransaction);
    assert_eq!(actual, expected);

    let actual = table("TxTest")
        .delete()
        .filter("id = 3")
        .execute(glue)
        .await;
    let expected = Ok(Payload::Delete(1));
    assert_eq!(actual, expected);

    let actual = table("TxTest")
        .select()
        .project("id, name")
        .execute(glue)
        .await;
    let expected = Ok(select!(
        id  | name
        I64 | Str;
        1     "Friday".to_owned();
        2     "Phone".to_owned()
    ));
    assert_eq!(actual, expected);

    let actual = rollback().execute(glue).await;
    let expected = Ok(Payload::Rollback);
    assert_eq!(actual, expected);

    let actual = table("TxTest")
        .select()
        .project("id, name")
        .execute(glue)
        .await;
    let expected = Ok(select!(
        id  | name
        I64 | Str;
        1     "Friday".to_owned();
        2     "Phone".to_owned();
        3     "Vienna".to_owned()
    ));
    assert_eq!(actual, expected);

    let actual = begin().execute(glue).await;
    let expected = Ok(Payload::StartTransaction);
    assert_eq!(actual, expected);

    let actual = table("TxTest")
        .delete()
        .filter("id = 3")
        .execute(glue)
        .await;
    let expected = Ok(Payload::Delete(1));
    assert_eq!(actual, expected);

    let actual = table("TxTest")
        .select()
        .project("id, name")
        .execute(glue)
        .await;
    let expected = Ok(select!(
        id  | name
        I64 | Str;
        1     "Friday".to_owned();
        2     "Phone".to_owned()
    ));
    assert_eq!(actual, expected);

    let actual = commit().execute(glue).await;
    let expected = Ok(Payload::Commit);
    assert_eq!(actual, expected);

    let actual = table("TxTest")
        .select()
        .project("id, name")
        .execute(glue)
        .await;
    let expected = Ok(select!(
        id  | name
        I64 | Str;
        1     "Friday".to_owned();
        2     "Phone".to_owned()
    ));
    assert_eq!(actual, expected);

    // UPDATE
    let actual = begin().execute(glue).await;
    let expected = Ok(Payload::StartTransaction);
    assert_eq!(actual, expected);

    let actual = table("TxTest")
        .update()
        .filter("id = 1")
        .set("name", "'Sunday'")
        .execute(glue)
        .await;
    let expected = Ok(Payload::Update(1));
    assert_eq!(actual, expected);

    let actual = table("TxTest")
        .select()
        .project("id, name")
        .execute(glue)
        .await;
    let expected = Ok(select!(
        id  | name
        I64 | Str;
        1     "Sunday".to_owned();
        2     "Phone".to_owned()
    ));
    assert_eq!(actual, expected);

    let actual = rollback().execute(glue).await;
    let expected = Ok(Payload::Rollback);
    assert_eq!(actual, expected);

    let actual = table("TxTest")
        .select()
        .project("id, name")
        .execute(glue)
        .await;
    let expected = Ok(select!(
        id  | name
        I64 | Str;
        1     "Friday".to_owned();
        2     "Phone".to_owned()
    ));
    assert_eq!(actual, expected);

    let actual = begin().execute(glue).await;
    let expected = Ok(Payload::StartTransaction);
    assert_eq!(actual, expected);

    let actual = table("TxTest")
        .update()
        .filter("id = 1")
        .set("name", "'Sunday'")
        .execute(glue)
        .await;
    let expected = Ok(Payload::Update(1));
    assert_eq!(actual, expected);

    let actual = table("TxTest")
        .select()
        .project("id, name")
        .execute(glue)
        .await;
    let expected = Ok(select!(
        id  | name
        I64 | Str;
        1     "Sunday".to_owned();
        2     "Phone".to_owned()
    ));
    assert_eq!(actual, expected);

    let actual = commit().execute(glue).await;
    let expected = Ok(Payload::Commit);
    assert_eq!(actual, expected);

    let actual = table("TxTest")
        .select()
        .project("id, name")
        .execute(glue)
        .await;
    let expected = Ok(select!(
        id  | name
        I64 | Str;
        1     "Sunday".to_owned();
        2     "Phone".to_owned()
    ));
    assert_eq!(actual, expected);
});
