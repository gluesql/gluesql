use gluesql::FromGlueRow;
use gluesql::row::{Payload, RowConversionError, SelectExt, Value};

#[derive(Debug, PartialEq, FromGlueRow)]
struct User {
    id: i64,
    name: String,
    email: Option<String>,
}

#[test]
fn rows_as_ok() {
    let payload = Payload::Select {
        labels: vec!["id".into(), "name".into(), "email".into()],
        rows: vec![
            vec![Value::I64(1), Value::Str("A".into()), Value::Null],
            vec![
                Value::I64(2),
                Value::Str("B".into()),
                Value::Str("b@x.com".into()),
            ],
        ],
    };
    let v: Vec<User> = payload.rows_as::<User>().unwrap();
    assert_eq!(v.len(), 2);
    assert_eq!(
        v[0],
        User {
            id: 1,
            name: "A".into(),
            email: None
        }
    );
    assert_eq!(v[1].email.as_deref(), Some("b@x.com"));
}

#[test]
fn one_as_ok() {
    let payload = Payload::Select {
        labels: vec!["id".into(), "name".into(), "email".into()],
        rows: vec![vec![Value::I64(7), Value::Str("Z".into()), Value::Null]],
    };
    let u: User = payload.one_as::<User>().unwrap();
    assert_eq!(u.id, 7);
}

#[test]
fn one_as_not_found() {
    let payload = Payload::Select {
        labels: vec!["id".into()],
        rows: vec![],
    };
    let err = payload.one_as::<User>().unwrap_err();
    assert!(matches!(err, RowConversionError::NotFound));
}

#[test]
fn one_as_more_than_one() {
    let payload = Payload::Select {
        labels: vec!["id".into(), "name".into(), "email".into()],
        rows: vec![
            vec![Value::I64(1), Value::Str("A".into()), Value::Null],
            vec![Value::I64(2), Value::Str("B".into()), Value::Null],
        ],
    };
    let err = payload.one_as::<User>().unwrap_err();
    if let RowConversionError::MoreThanOneRow { got } = err {
        assert_eq!(got, 2);
    } else {
        panic!("expected MoreThanOneRow");
    }
}

#[allow(dead_code)]
#[derive(Debug, FromGlueRow)]
struct NeedsName {
    id: i64,
    name: String,
}

#[test]
fn missing_column() {
    let payload = Payload::Select {
        labels: vec!["id".into()], // name missing
        rows: vec![vec![Value::I64(1)]],
    };
    let err = payload.rows_as::<NeedsName>().unwrap_err();
    if let RowConversionError::MissingColumn { field, column } = err {
        assert_eq!(field, "name");
        assert_eq!(column, "name");
    } else {
        panic!("expected MissingColumn");
    }
}

#[allow(dead_code)]
#[derive(Debug, FromGlueRow)]
struct NonNullable {
    id: i64,
    name: String, // name cannot be Null
}

#[test]
fn null_not_allowed() {
    let payload = Payload::Select {
        labels: vec!["id".into(), "name".into()],
        rows: vec![vec![Value::I64(1), Value::Null]],
    };
    let err = payload.rows_as::<NonNullable>().unwrap_err();
    match err {
        RowConversionError::NullNotAllowed { .. } => {}
        _ => panic!("expected NullNotAllowed"),
    }
}

#[allow(dead_code)]
#[derive(Debug, FromGlueRow)]
struct Price {
    price: f64,
}

#[test]
fn type_mismatch() {
    let payload = Payload::Select {
        labels: vec!["price".into()],
        rows: vec![vec![Value::Str("9.99".into())]], // string -> f64 forbidden
    };
    let err = payload.rows_as::<Price>().unwrap_err();
    if let RowConversionError::TypeMismatch { expected, .. } = err {
        assert_eq!(expected, "f64");
    } else {
        panic!("expected TypeMismatch");
    }
}

#[derive(Debug, PartialEq, FromGlueRow)]
struct Order {
    #[glue(rename = "order_id")]
    id: i64,
    total: f64,
}

#[test]
fn rename_ok() {
    let payload = Payload::Select {
        labels: vec!["order_id".into(), "total".into()],
        rows: vec![vec![Value::I64(10), Value::F64(12.5)]],
    };
    let o: Order = payload.one_as::<Order>().unwrap();
    assert_eq!(o.id, 10);
}

#[test]
fn not_select_payload() {
    let payload = Payload::Insert(1);
    let err = payload.rows_as::<User>().unwrap_err();
    assert!(matches!(err, RowConversionError::NotSelectPayload));
}
