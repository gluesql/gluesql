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

    pub(crate) fn into_expr_for_placeholder(self, _index: usize) -> crate::result::Result<Expr> {
        Ok(self.into_expr())
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
        ParamLiteral::Literal(into_number_literal(value))
    }
}

impl From<f64> for ParamLiteral {
    fn from(value: f64) -> Self {
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
                expr: Box::new(ParamLiteral::Literal(into_number_literal(
                    micros / 1_000_000,
                ))),
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
        crate::ast::{AstLiteral, Expr},
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
    }

    #[test]
    fn converts_option() {
        let expr = ParamLiteral::from(Some(1_i32)).into_expr();
        assert_eq!(expr, Expr::Literal(AstLiteral::Number(1.into())));

        let expr = ParamLiteral::from(None::<i32>).into_expr();
        assert_eq!(expr, Expr::Literal(AstLiteral::Null));
    }
}
