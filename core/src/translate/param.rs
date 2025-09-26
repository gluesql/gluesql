use {
    crate::{
        ast::{AstLiteral, DataType, DateTimeField, Expr},
        data::{Interval, Point},
    },
    bigdecimal::BigDecimal,
    chrono::{NaiveDate, NaiveDateTime, NaiveTime, TimeZone, Utc},
    rust_decimal::Decimal,
    std::net::IpAddr,
    uuid::Uuid,
};

#[derive(Debug, Clone)]
pub enum ParamLiteral {
    Literal(AstLiteral),
    TypedString {
        data_type: DataType,
        value: String,
    },
    Interval {
        expr: Box<ParamLiteral>,
        leading_field: Option<DateTimeField>,
        last_field: Option<DateTimeField>,
    },
}

impl ParamLiteral {
    pub const fn null() -> Self {
        Self::Literal(AstLiteral::Null)
    }

    pub fn into_expr(self) -> Expr {
        match self {
            ParamLiteral::Literal(literal) => Expr::Literal(literal),
            ParamLiteral::TypedString { data_type, value } => {
                Expr::TypedString { data_type, value }
            }
            ParamLiteral::Interval {
                expr,
                leading_field,
                last_field,
            } => Expr::Interval {
                expr: Box::new(expr.into_expr()),
                leading_field,
                last_field,
            },
        }
    }
}

fn into_number_literal(value: impl ToString) -> AstLiteral {
    let number = value
        .to_string()
        .parse::<BigDecimal>()
        .expect("failed to convert number into BigDecimal");

    AstLiteral::Number(number)
}

impl From<bool> for ParamLiteral {
    fn from(value: bool) -> Self {
        ParamLiteral::Literal(AstLiteral::Boolean(value))
    }
}

macro_rules! impl_from_integer {
    ($($ty:ty),+ $(,)?) => {
        $(
            impl From<$ty> for ParamLiteral {
                fn from(value: $ty) -> Self {
                    ParamLiteral::Literal(into_number_literal(value))
                }
            }
        )+
    };
}

impl_from_integer!(
    i8, i16, i32, i64, i128, isize, u8, u16, u32, u64, u128, usize
);

impl From<f32> for ParamLiteral {
    fn from(value: f32) -> Self {
        if !value.is_finite() {
            return ParamLiteral::null();
        }
        ParamLiteral::Literal(into_number_literal(value))
    }
}

impl From<f64> for ParamLiteral {
    fn from(value: f64) -> Self {
        if !value.is_finite() {
            return ParamLiteral::null();
        }
        ParamLiteral::Literal(into_number_literal(value))
    }
}

impl From<Decimal> for ParamLiteral {
    fn from(value: Decimal) -> Self {
        ParamLiteral::Literal(into_number_literal(value))
    }
}

impl From<String> for ParamLiteral {
    fn from(value: String) -> Self {
        ParamLiteral::Literal(AstLiteral::QuotedString(value))
    }
}

impl From<&str> for ParamLiteral {
    fn from(value: &str) -> Self {
        ParamLiteral::Literal(AstLiteral::QuotedString(value.to_owned()))
    }
}

impl From<Vec<u8>> for ParamLiteral {
    fn from(value: Vec<u8>) -> Self {
        ParamLiteral::Literal(AstLiteral::HexString(hex::encode(value)))
    }
}

impl From<&[u8]> for ParamLiteral {
    fn from(value: &[u8]) -> Self {
        ParamLiteral::Literal(AstLiteral::HexString(hex::encode(value)))
    }
}

impl From<IpAddr> for ParamLiteral {
    fn from(value: IpAddr) -> Self {
        ParamLiteral::Literal(AstLiteral::QuotedString(value.to_string()))
    }
}

impl From<NaiveDate> for ParamLiteral {
    fn from(value: NaiveDate) -> Self {
        ParamLiteral::TypedString {
            data_type: DataType::Date,
            value: value.to_string(),
        }
    }
}

impl From<NaiveTime> for ParamLiteral {
    fn from(value: NaiveTime) -> Self {
        ParamLiteral::TypedString {
            data_type: DataType::Time,
            value: value.to_string(),
        }
    }
}

impl From<NaiveDateTime> for ParamLiteral {
    fn from(value: NaiveDateTime) -> Self {
        ParamLiteral::TypedString {
            data_type: DataType::Timestamp,
            value: Utc.from_utc_datetime(&value).to_string(),
        }
    }
}

impl From<Uuid> for ParamLiteral {
    fn from(value: Uuid) -> Self {
        ParamLiteral::Literal(AstLiteral::QuotedString(value.hyphenated().to_string()))
    }
}

impl From<Point> for ParamLiteral {
    fn from(value: Point) -> Self {
        ParamLiteral::Literal(AstLiteral::QuotedString(value.to_string()))
    }
}

impl From<Interval> for ParamLiteral {
    fn from(value: Interval) -> Self {
        match value {
            Interval::Month(months) => ParamLiteral::Interval {
                expr: Box::new(ParamLiteral::Literal(into_number_literal(months))),
                leading_field: Some(DateTimeField::Month),
                last_field: None,
            },
            Interval::Microsecond(micros) => ParamLiteral::Interval {
                expr: Box::new(ParamLiteral::Literal(into_number_literal(Decimal::new(
                    micros, 6,
                )))),
                leading_field: Some(DateTimeField::Second),
                last_field: None,
            },
        }
    }
}

impl<T> From<Option<T>> for ParamLiteral
where
    T: Into<ParamLiteral>,
{
    fn from(value: Option<T>) -> Self {
        value.map(Into::into).unwrap_or_else(ParamLiteral::null)
    }
}

#[macro_export]
macro_rules! params {
    ($($expr:expr),* $(,)?) => {
        vec![
            $(
                $crate::translate::ParamLiteral::from($expr)
            ),*
        ]
    };
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            ast::{AstLiteral, Expr},
            data::Point,
        },
        bigdecimal::BigDecimal,
        chrono::{NaiveDate, NaiveTime},
        rust_decimal::Decimal,
        std::{net::IpAddr, str::FromStr},
        uuid::Uuid,
    };

    #[test]
    fn converts_basic_literals() {
        let literal = ParamLiteral::from(true).into_expr();
        assert_eq!(literal, Expr::Literal(AstLiteral::Boolean(true)));

        let literal = ParamLiteral::from(42_i64).into_expr();
        assert_eq!(literal, Expr::Literal(AstLiteral::Number(42.into())));

        let literal = ParamLiteral::from("glue").into_expr();
        assert_eq!(
            literal,
            Expr::Literal(AstLiteral::QuotedString("glue".to_owned()))
        );

        let literal = ParamLiteral::from(String::from("owned"));
        assert_eq!(
            literal.into_expr(),
            Expr::Literal(AstLiteral::QuotedString("owned".to_owned()))
        );

        let literal = ParamLiteral::from(3_i16).into_expr();
        assert_eq!(literal, Expr::Literal(AstLiteral::Number(3.into())));

        let literal = ParamLiteral::from(7_u32).into_expr();
        assert_eq!(literal, Expr::Literal(AstLiteral::Number(7.into())));
    }

    #[test]
    fn converts_typed_literals() {
        let date = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
        let expr = ParamLiteral::from(date).into_expr();
        assert_eq!(
            expr,
            Expr::TypedString {
                data_type: DataType::Date,
                value: "2024-01-15".to_owned(),
            }
        );

        let time = NaiveTime::from_hms_opt(9, 45, 30).unwrap();
        let expr = ParamLiteral::from(time).into_expr();
        assert_eq!(
            expr,
            Expr::TypedString {
                data_type: DataType::Time,
                value: "09:45:30".to_owned(),
            }
        );

        let timestamp = NaiveDate::from_ymd_opt(2024, 1, 15)
            .unwrap()
            .and_hms_opt(12, 0, 0)
            .unwrap();
        let expr = ParamLiteral::from(timestamp).into_expr();
        assert_eq!(
            expr,
            Expr::TypedString {
                data_type: DataType::Timestamp,
                value: "2024-01-15 12:00:00 UTC".to_owned(),
            }
        );
    }

    #[test]
    fn converts_interval() {
        let expr = ParamLiteral::from(Interval::Month(2)).into_expr();
        assert_eq!(
            expr,
            Expr::Interval {
                expr: Box::new(Expr::Literal(AstLiteral::Number(2.into()))),
                leading_field: Some(DateTimeField::Month),
                last_field: None,
            }
        );

        let expr = ParamLiteral::from(Interval::Microsecond(1_500_000)).into_expr();
        assert_eq!(
            expr,
            Expr::Interval {
                expr: Box::new(Expr::Literal(AstLiteral::Number(
                    BigDecimal::from_str("1.5").unwrap(),
                ))),
                leading_field: Some(DateTimeField::Second),
                last_field: None,
            }
        );
    }

    #[test]
    fn converts_option() {
        let expr = ParamLiteral::from(Some(1_i32)).into_expr();
        assert_eq!(expr, Expr::Literal(AstLiteral::Number(1.into())));

        let expr = ParamLiteral::from(None::<i32>).into_expr();
        assert_eq!(expr, Expr::Literal(AstLiteral::Null));
    }

    #[test]
    fn converts_scalars_and_structs() {
        let expr = ParamLiteral::from(1.25_f64).into_expr();
        assert_eq!(
            expr,
            Expr::Literal(AstLiteral::Number(BigDecimal::from_str("1.25").unwrap()))
        );

        let expr = ParamLiteral::from(f32::NAN).into_expr();
        assert_eq!(expr, Expr::Literal(AstLiteral::Null));

        let expr = ParamLiteral::from(f64::INFINITY).into_expr();
        assert_eq!(expr, Expr::Literal(AstLiteral::Null));

        let expr = ParamLiteral::from(1.5_f32).into_expr();
        assert_eq!(
            expr,
            Expr::Literal(AstLiteral::Number(BigDecimal::from_str("1.5").unwrap()))
        );

        let expr = ParamLiteral::from(Decimal::new(345, 2)).into_expr();
        assert_eq!(
            expr,
            Expr::Literal(AstLiteral::Number(BigDecimal::from_str("3.45").unwrap()))
        );

        let expr = ParamLiteral::from(vec![0x12_u8, 0xAB]).into_expr();
        assert_eq!(
            expr,
            Expr::Literal(AstLiteral::HexString("12ab".to_owned()))
        );

        let bytes = [0xCD_u8, 0xEF];
        let expr = ParamLiteral::from(bytes.as_slice()).into_expr();
        assert_eq!(
            expr,
            Expr::Literal(AstLiteral::HexString("cdef".to_owned()))
        );

        let ip = IpAddr::from_str("127.0.0.1").unwrap();
        let expr = ParamLiteral::from(ip).into_expr();
        assert_eq!(
            expr,
            Expr::Literal(AstLiteral::QuotedString("127.0.0.1".to_owned()))
        );

        let uuid = Uuid::parse_str("123e4567-e89b-12d3-a456-426614174000").unwrap();
        let expr = ParamLiteral::from(uuid).into_expr();
        assert_eq!(
            expr,
            Expr::Literal(AstLiteral::QuotedString(
                "123e4567-e89b-12d3-a456-426614174000".to_owned()
            ))
        );

        let point = Point::new(1.0, 2.0);
        let expr = ParamLiteral::from(point).into_expr();
        assert_eq!(
            expr,
            Expr::Literal(AstLiteral::QuotedString("POINT(1 2)".to_owned()))
        );
    }

    #[test]
    fn params_macro_collects_literals() {
        let params = crate::params![1_i64, "Glue", Some(false), None::<i32>];

        assert_eq!(params.len(), 4);

        assert_eq!(
            params[0].clone().into_expr(),
            Expr::Literal(AstLiteral::Number(1.into()))
        );
        assert_eq!(
            params[1].clone().into_expr(),
            Expr::Literal(AstLiteral::QuotedString("Glue".to_owned()))
        );
        assert_eq!(
            params[2].clone().into_expr(),
            Expr::Literal(AstLiteral::Boolean(false))
        );
        assert_eq!(
            params[3].clone().into_expr(),
            Expr::Literal(AstLiteral::Null)
        );
    }
}
