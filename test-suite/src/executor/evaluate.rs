// ...existing code...

#[test]
fn test_evaluated_try_into_f64() -> crate::result::Result<()> {
    use gluesql::executor::evaluate::evaluated::Evaluated;
    use gluesql::data::{Literal, Value};

    let evaluated = Evaluated::Literal(Literal::Number("123.45".to_owned()));
    assert_eq!(evaluated.try_into(), Ok(123.45_f64));

    let evaluated = Evaluated::Value(Value::I64(42));
    assert_eq!(evaluated.try_into(), Ok(42.0_f64));

    Ok(())
}

#[test]
fn test_evaluated_try_into_i64() -> crate::result::Result<()> {
    use gluesql::executor::evaluate::evaluated::Evaluated;
    use gluesql::data::{Literal, Value};

    let evaluated = Evaluated::Literal(Literal::Number("123".to_owned()));
    assert_eq!(evaluated.try_into(), Ok(123_i64));

    let evaluated = Evaluated::Value(Value::F64(123.0));
    assert_eq!(evaluated.try_into(), Ok(123_i64));

    Ok(())
}

#[test]
fn test_evaluated_try_into_string() -> crate::result::Result<()> {
    use gluesql::executor::evaluate::evaluated::Evaluated;
    use gluesql::data::{Literal, Value};

    let evaluated = Evaluated::Literal(Literal::Text("hello".to_owned()));
    assert_eq!(evaluated.try_into(), Ok("hello".to_owned()));

    let evaluated = Evaluated::Value(Value::I64(42));
    assert_eq!(evaluated.try_into(), Ok("42".to_owned()));

    Ok(())
}

// ...existing code...

