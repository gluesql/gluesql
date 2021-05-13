use {
    super::TranslateError,
    crate::{
        ast::{AstLiteral, DateTimeField},
        result::Result,
    },
    sqlparser::ast::{DateTimeField as SqlDateTimeField, Value as SqlValue},
};

pub fn translate_ast_literal(sql_value: &SqlValue) -> Result<AstLiteral> {
    Ok(match sql_value {
        SqlValue::Boolean(v) => AstLiteral::Boolean(*v),
        SqlValue::Number(v, _) => AstLiteral::Number(v.clone()),
        SqlValue::SingleQuotedString(v) => AstLiteral::QuotedString(v.clone()),
        SqlValue::Interval {
            value,
            leading_field,
            last_field,
            ..
        } => AstLiteral::Interval {
            value: value.to_owned(),
            leading_field: leading_field.as_ref().map(translate_datetime_field),
            last_field: last_field.as_ref().map(translate_datetime_field),
        },
        SqlValue::Null => AstLiteral::Null,
        _ => {
            return Err(TranslateError::UnsupportedAstLiteral(sql_value.to_string()).into());
        }
    })
}

fn translate_datetime_field(sql_datetime_field: &SqlDateTimeField) -> DateTimeField {
    match sql_datetime_field {
        SqlDateTimeField::Year => DateTimeField::Year,
        SqlDateTimeField::Month => DateTimeField::Month,
        SqlDateTimeField::Day => DateTimeField::Day,
        SqlDateTimeField::Hour => DateTimeField::Hour,
        SqlDateTimeField::Minute => DateTimeField::Minute,
        SqlDateTimeField::Second => DateTimeField::Second,
    }
}
