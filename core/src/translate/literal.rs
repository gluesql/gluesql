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

#[cfg(test)]
mod tests {
    use crate::{
        parse_sql::parse_expr,
        translate::{NO_PARAMS, TranslateError, translate_expr},
    };

    #[test]
    fn dollar_quoted_string_literal_rejected() {
        // PostgreSqlDialect tokenizes `$$..$$` into Value::DollarQuotedString,
        // which translate_literal does not support.
        let actual = parse_expr("$$abc$$").and_then(|parsed| translate_expr(&parsed, NO_PARAMS));
        let expected = Err(TranslateError::UnsupportedLiteral("$$abc$$".to_owned()).into());
        assert_eq!(actual, expected);
    }
}
