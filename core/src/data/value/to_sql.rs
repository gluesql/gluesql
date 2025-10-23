use {
    super::Value,
    crate::{
        ast::ToSql,
        chrono::{TimeZone, Utc},
    },
    bigdecimal::{BigDecimal, FromPrimitive},
    serde_json::Value as JsonValue,
    uuid::Uuid,
};

impl ToSql for Value {
    fn to_sql(&self) -> String {
        match self {
            Value::Bool(value) => match value {
                true => "TRUE".to_owned(),
                false => "FALSE".to_owned(),
            },
            Value::I8(value) => value.to_string(),
            Value::I16(value) => value.to_string(),
            Value::I32(value) => value.to_string(),
            Value::I64(value) => value.to_string(),
            Value::I128(value) => value.to_string(),
            Value::U8(value) => value.to_string(),
            Value::U16(value) => value.to_string(),
            Value::U32(value) => value.to_string(),
            Value::U64(value) => value.to_string(),
            Value::U128(value) => value.to_string(),
            Value::F32(value) => format_float(BigDecimal::from_f32(*value), f64::from(*value)),
            Value::F64(value) => format_float(BigDecimal::from_f64(*value), *value),
            Value::Decimal(value) => value.to_string(),
            Value::Str(value) => quote(value),
            Value::Bytea(bytes) => quote(&hex::encode(bytes)),
            Value::Inet(addr) => quote(&addr.to_string()),
            Value::Date(value) => format!("DATE {}", quote(&value.to_string())),
            Value::Timestamp(value) => format!(
                "TIMESTAMP {}",
                quote(&Utc.from_utc_datetime(value).to_string())
            ),
            Value::Time(value) => format!("TIME {}", quote(&value.to_string())),
            Value::Interval(interval) => format!("INTERVAL {}", interval.to_sql_str()),
            Value::Uuid(value) => quote(&Uuid::from_u128(*value).hyphenated().to_string()),
            Value::Map(map) => quote(&json_from_value(Value::Map(map.clone()), "{}")),
            Value::List(list) => quote(&json_from_value(Value::List(list.clone()), "[]")),
            Value::Point(point) => quote(&point.to_string()),
            Value::Null => "NULL".to_owned(),
        }
    }
}

fn quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn format_float(from_big_decimal: Option<BigDecimal>, fallback: f64) -> String {
    match from_big_decimal {
        Some(decimal) => decimal.to_string(),
        None => fallback.to_string(),
    }
}

fn json_from_value(value: Value, default: &str) -> String {
    JsonValue::try_from(value)
        .map(|json| json.to_string())
        .unwrap_or_else(|_| default.to_owned())
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::data::{Interval, Value, point::Point, value::uuid::parse_uuid},
        chrono::{NaiveDate, NaiveDateTime, NaiveTime},
        std::collections::BTreeMap,
    };

    #[test]
    fn scalar_values_to_sql() {
        assert_eq!(Value::Bool(true).to_sql(), "TRUE");
        assert_eq!(Value::I32(-42).to_sql(), "-42");
        assert_eq!(Value::F64(3.5).to_sql(), "3.5");
        assert_eq!(Value::Str("can't".into()).to_sql(), "'can''t'");
        assert_eq!(Value::Bytea(vec![0xde, 0xad]).to_sql(), "'dead'");

        let uuid = parse_uuid("b6631ba6-9329-4e9d-b5ba-9b405df82d47").unwrap();
        assert_eq!(
            Value::Uuid(uuid).to_sql(),
            "'b6631ba6-9329-4e9d-b5ba-9b405df82d47'"
        );

        assert_eq!(
            Value::Date(NaiveDate::from_ymd_opt(2024, 1, 30).unwrap()).to_sql(),
            "DATE '2024-01-30'"
        );
        assert_eq!(
            Value::Timestamp(NaiveDateTime::new(
                NaiveDate::from_ymd_opt(2024, 1, 30).unwrap(),
                NaiveTime::from_hms_opt(12, 5, 10).unwrap(),
            ),)
            .to_sql(),
            "TIMESTAMP '2024-01-30 12:05:10 UTC'"
        );
        assert_eq!(
            Value::Time(NaiveTime::from_hms_opt(23, 59, 59).unwrap()).to_sql(),
            "TIME '23:59:59'"
        );
    }

    #[test]
    fn complex_values_to_sql() {
        assert_eq!(
            Value::Interval(Interval::Month(15)).to_sql(),
            "INTERVAL '1-3' YEAR TO MONTH"
        );

        let mut map = BTreeMap::new();
        map.insert("flag".to_owned(), Value::Bool(true));
        assert_eq!(Value::Map(map).to_sql(), "'{\"flag\":true}'");

        let list = vec![Value::I32(1), Value::I32(2)];
        assert_eq!(Value::List(list).to_sql(), "'[1,2]'");

        assert_eq!(
            Value::Point(Point::new(1.1, 2.2)).to_sql(),
            "'POINT(1.1 2.2)'"
        );
        assert_eq!(Value::Null.to_sql(), "NULL");
    }
}
