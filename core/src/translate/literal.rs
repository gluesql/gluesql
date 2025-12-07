use {
    super::TranslateError,
    crate::{
        ast::{DateTimeField, Literal, TrimWhereField},
        result::Result,
    },
    sqlparser::ast::{
        DateTimeField as SqlDateTimeField, TrimWhereField as SqlTrimWhereField, Value as SqlValue,
    },
};

pub fn translate_literal(sql_value: &SqlValue) -> Result<Literal> {
    Ok(match sql_value {
        SqlValue::Boolean(v) => Literal::Boolean(*v),
        SqlValue::Number(v, _) => Literal::Number(v.clone()),
        SqlValue::SingleQuotedString(v) => Literal::QuotedString(v.clone()),
        SqlValue::HexStringLiteral(v) => Literal::HexString(v.clone()),
        SqlValue::Null => Literal::Null,
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
