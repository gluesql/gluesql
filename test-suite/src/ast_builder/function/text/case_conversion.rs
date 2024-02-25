use {
    crate::*,
    gluesql_core::{
        ast_builder::{function as f, *},
        executor::Payload,
        prelude::Value::*,
    },
};

test_case!(case_conversion, {
    let glue = get_glue!();

    let actual = table("Item")
        .create_table()
        .add_column("id INTEGER PRIMARY KEY")
        .add_column("name TEXT DEFAULT UPPER('abc')")
        .add_column("opt_name TEXT DEFAULT LOWER('ABC')")
        .add_column("capped_name TEXT DEFAULT 'pascal'")
        .execute(glue)
        .await;
    let expected = Ok(Payload::Create);
    assert_eq!(actual, expected, "create table - Item");

    let actual = table("Item")
        .insert()
        .values(vec![
            vec![num(1), text("abcd"), text("efgi"), text("h/i jk")],
            vec![num(2), text("Abcd"), null(), null()],
            vec![num(3), text("ABCD"), text("EfGi"), text("H/I JK")],
        ])
        .execute(glue)
        .await;
    let expected = Ok(Payload::Insert(3));
    assert_eq!(actual, expected, "insert - Item");

    //check upper,lower case
    let actual = table("Item")
        .select()
        .filter(col("name").lower().eq("'abcd'"))
        .project("name")
        .project(f::lower("name"))
        .execute(glue)
        .await;
    let expected = Ok(select!(
        name                | r#"LOWER("name")"#
        Str                 | Str;
        "abcd".to_owned()    "abcd".to_owned();
        "Abcd".to_owned()    "abcd".to_owned();
        "ABCD".to_owned()    "abcd".to_owned()
    ));
    assert_eq!(actual, expected, "check lower case");

    let actual = table("Item")
        .select()
        .project(col("name").lower())
        .project(col("name").upper())
        .execute(glue)
        .await;
    let expected = Ok(select!(
        r#"LOWER("name")"#  | "UPPER(\"name\")"
        Str                 | Str;
        "abcd".to_owned()    "ABCD".to_owned();
        "abcd".to_owned()    "ABCD".to_owned();
        "abcd".to_owned()    "ABCD".to_owned()
    ));
    assert_eq!(actual, expected, "check upper case");

    let actual = table("Item")
        .select()
        .project(col("opt_name").lower())
        .project(col("opt_name").upper())
        .execute(glue)
        .await;
    let expected = Ok(select_with_null!(
        r#"LOWER("opt_name")"#  | "UPPER(\"opt_name\")";
        Str("efgi".to_owned())    Str("EFGI".to_owned());
        Null                      Null;
        Str("efgi".to_owned())    Str("EFGI".to_owned())
    ));
    assert_eq!(actual, expected, "check upper & lower null case");

    //check initcap case
    let actual = table("Item")
        .select()
        .filter(col("capped_name").initcap().eq("'H/I Jk'"))
        .project("capped_name")
        .execute(glue)
        .await;
    let expected = Ok(select!(
        capped_name
        Str;
        "h/i jk".to_owned();
        "H/I JK".to_owned()
    ));
    assert_eq!(actual, expected, "check initcap case");
});
