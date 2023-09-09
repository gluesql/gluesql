use {
    crate::*,
    gluesql_core::{
        ast_builder::{function as f, *},
        prelude::{Payload, Value::*},
    },
};

test_case!(current_date_and_time, {
    macro_rules! t {
        ($timestamp: expr) => {
            $timestamp.parse().unwrap()
        };
    }

    let glue = get_glue!();

    let actual = table("Record")
        .create_table()
        .add_column("id INTEGER PRIMARY KEY")
        .add_column("time_stamp TIMESTAMP")
        .execute(glue)
        .await;
    let expected = Ok(Payload::Create);
    assert_eq!(actual, expected, "create table - Record");

    let actual = table("Record")
        .insert()
        .values(vec![
            "1, '2022-12-23T05:30:11.164932863'",
            "2, NOW()",
            "3, '9999-12-31T23:59:40.364832862'",
        ])
        .execute(glue)
        .await;
    let expected = Ok(Payload::Insert(3));
    assert_eq!(actual, expected, "insert - Record");

    // Now
    let actual = table("Record")
        .select()
        .filter(col("time_stamp").gt(f::now()))
        .project("id, time_stamp")
        .execute(glue)
        .await;
    let expected = Ok(select!(
        id  | time_stamp
        I64 | Timestamp;
        3     t!("9999-12-31T23:59:40.364832862")
    ));
    assert_eq!(actual, expected, "now");
});
