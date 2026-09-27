use {
    gluesql_core::{ast::Expr, data::Value, row_conversion::ToGlueRow, translate::ParamLiteral},
    gluesql_macros::{FromGlueRow, ToGlueRow},
};

#[derive(ToGlueRow)]
struct Item {
    id: i64,
    #[glue(rename = "title")]
    name: String,
    price_cents: i64,
    in_stock: Option<bool>,
}

#[test]
fn glue_columns_honors_rename() {
    assert_eq!(
        Item::glue_columns(),
        &["id", "title", "price_cents", "in_stock"]
    );
}

#[test]
fn to_glue_row_converts_values() {
    let item = Item {
        id: 1,
        name: "glue".to_owned(),
        price_cents: 100,
        in_stock: Some(true),
    };

    let exprs = item
        .to_glue_row()
        .into_iter()
        .map(ParamLiteral::into_expr)
        .collect::<Vec<_>>();

    assert_eq!(
        exprs,
        vec![
            Expr::Value(Value::I64(1)),
            Expr::Value(Value::Str("glue".to_owned())),
            Expr::Value(Value::I64(100)),
            Expr::Value(Value::Bool(true)),
        ]
    );
}

#[test]
fn to_glue_row_none_becomes_null() {
    let item = Item {
        id: 2,
        name: "sql".to_owned(),
        price_cents: 200,
        in_stock: None,
    };

    let exprs = item
        .to_glue_row()
        .into_iter()
        .map(ParamLiteral::into_expr)
        .collect::<Vec<_>>();

    assert_eq!(exprs[3], Expr::Value(Value::Null));
}

#[derive(Debug, PartialEq, FromGlueRow, ToGlueRow)]
struct User {
    id: i64,
    #[glue(rename = "full_name")]
    name: String,
    email: Option<String>,
}

#[test]
fn values_from_round_trip_with_memory_storage() {
    use {
        gluesql_core::{
            prelude::Glue,
            query_builder::{Execute, table},
            row_conversion::SelectResultExt,
        },
        gluesql_memory_storage::MemoryStorage,
    };

    let storage = MemoryStorage::default();
    let mut glue = Glue::new(storage);

    glue.execute("CREATE TABLE users (id INTEGER, full_name TEXT, email TEXT);")
        .unwrap();

    let users = vec![
        User {
            id: 1,
            name: "A".to_owned(),
            email: None,
        },
        User {
            id: 2,
            name: "B".to_owned(),
            email: Some("b@x.com".to_owned()),
        },
    ];

    table("users")
        .insert()
        .values_from(&users)
        .unwrap()
        .execute(&mut glue)
        .unwrap();

    let fetched: Vec<User> = glue
        .execute("SELECT id, full_name, email FROM users ORDER BY id;")
        .rows_as::<User>()
        .unwrap();

    assert_eq!(fetched, users);
}

#[derive(ToGlueRow)]
struct GenericRecord<'a, T>
where
    T: Clone + gluesql_core::translate::IntoParamLiteral,
{
    name: &'a str,
    value: T,
}

#[test]
fn to_glue_row_supports_generics_and_lifetimes() {
    let rec = GenericRecord {
        name: "test",
        value: 42_i64,
    };
    assert_eq!(GenericRecord::<i64>::glue_columns(), &["name", "value"]);
    let exprs = rec
        .to_glue_row()
        .into_iter()
        .map(ParamLiteral::into_expr)
        .collect::<Vec<_>>();
    assert_eq!(
        exprs,
        vec![
            Expr::Value(Value::Str("test".to_owned())),
            Expr::Value(Value::I64(42)),
        ]
    );
}

#[derive(ToGlueRow)]
struct ConstGenericRecord<T, const N: usize>
where
    T: Clone + gluesql_core::translate::IntoParamLiteral,
{
    tag: String,
    payload: T,
}

#[test]
fn to_glue_row_supports_const_generics() {
    let rec = ConstGenericRecord::<i64, 10> {
        tag: "const_tag".to_owned(),
        payload: 99_i64,
    };
    assert_eq!(
        ConstGenericRecord::<i64, 10>::glue_columns(),
        &["tag", "payload"]
    );
    let exprs = rec
        .to_glue_row()
        .into_iter()
        .map(ParamLiteral::into_expr)
        .collect::<Vec<_>>();
    assert_eq!(
        exprs,
        vec![
            Expr::Value(Value::Str("const_tag".to_owned())),
            Expr::Value(Value::I64(99)),
        ]
    );
}
