use {
    super::TranslateError,
    crate::{
        ast::{DateTimeField, Expr, Literal, TrimWhereField},
        data::Value,
        result::Result,
    },
    sqlparser::ast::{
        DateTimeField as SqlDateTimeField, TrimWhereField as SqlTrimWhereField, Value as SqlValue,
    },
};

pub fn translate_literal(sql_value: &SqlValue) -> Result<Expr> {
    Ok(match sql_value {
        SqlValue::Boolean(v) => Expr::Value(Value::Bool(*v)),
        SqlValue::Number(v, _) => Expr::Literal(Literal::Number(v.clone())),
        SqlValue::SingleQuotedString(v) => Expr::Literal(Literal::QuotedString(v.clone())),
        SqlValue::HexStringLiteral(v) => {
            let bytes =
                hex::decode(v).map_err(|_| TranslateError::FailedToDecodeHexString(v.clone()))?;
            Expr::Value(Value::Bytea(bytes))
        }
        SqlValue::Null => Expr::Value(Value::Null),
        _ => {
            return Err(TranslateError::UnsupportedLiteral(sql_value.to_string()).into());
        }
    })
}

pub fn translate_datetime_field(sql_datetime_field: &SqlDateTimeField) -> Result<DateTimeField> {
    Ok(match sql_datetime_field {
        SqlDateTimeField::Year => DateTimeField::Year,
        SqlDateTimeField::Month => DateTimeField::Month,
        SqlDateTimeField::Day => DateTimeField::Day,
        SqlDateTimeField::Hour => DateTimeField::Hour,
        SqlDateTimeField::Minute => DateTimeField::Minute,
        SqlDateTimeField::Second => DateTimeField::Second,
        _ => {
            return Err(
                TranslateError::UnsupportedDateTimeField(sql_datetime_field.to_string()).into(),
            );
        }
    })
}

pub fn translate_trim_where_field(sql_trim_where_field: SqlTrimWhereField) -> TrimWhereField {
    use TrimWhereField::*;
    match sql_trim_where_field {
        SqlTrimWhereField::Both => Both,
        SqlTrimWhereField::Leading => Leading,
        SqlTrimWhereField::Trailing => Trailing,
    }
}
