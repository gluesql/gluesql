use {
    crate::*,
    gluesql_core::{
        ast::DataType,
        ast_builder::{function as f, *},
        executor::Payload,
    },
};

test_case!(current_time, {
    let glue = get_glue!();

    // create table - Record
    let actual = table("Record")
        .create_table()
        .add_column("id INTEGER PRIMARY KEY")
        .add_column("time_stamp TIME")
        .execute(glue)
        .await;
    let expected = Ok(Payload::Create);
    assert_eq!(actual, expected, "create table - Record");

    // insert into Record
    let actual = table("Record")
        .insert()
        .values(vec!["1, CURRENT_TIME()"])
        .execute(glue)
        .await;
    let expected = Ok(Payload::Insert(1));
    assert_eq!(actual, expected, "insert into Record");

    // check if current_time() returns a Time type
    let actual = values(vec![vec![f::current_time()]]).execute(glue).await;
    type_match(&[DataType::Time], actual);
});
