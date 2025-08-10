use {
    crate::*,
    gluesql_core::{
        ast_builder::{function as f, *},
        prelude::{Payload, Value::*},
    },
};

test_case!(current_date, {
    macro_rules! date {
        ($date: expr) => {
            $date.parse().unwrap()
        };
    }

    let glue = get_glue!();

    let actual = table("Record")
        .create_table()
        .add_column("id INTEGER PRIMARY KEY")
        .add_column("date_stamp DATE")
        .execute(glue)
        .await;
    let expected = Ok(Payload::Create);
    assert_eq!(actual, expected, "create table - Record");

    let actual = table("Record")
        .insert()
        .values(vec![
            "1, '2022-12-23'",
            "2, CURRENT_DATE()",
            "3, '9999-12-31'",
        ])
        .execute(glue)
        .await;
    let expected = Ok(Payload::Insert(3));
    assert_eq!(actual, expected, "insert - Record");

    // Current Date
    let actual = table("Record")
        .select()
        .filter(col("date_stamp").gt(f::current_date()))
        .project("id, date_stamp")
        .execute(glue)
        .await;
    let expected = Ok(select!(
        id  | date_stamp
        I64 | Date;
        3     date!("9999-12-31")
    ));
    assert_eq!(actual, expected, "current date");
});
