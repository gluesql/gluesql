use {
    super::TranslateError,
    crate::{
        ast::{DataType, DateTimeField, Expr, Literal},
        data::{Interval, Point, Value},
    },
    bigdecimal::BigDecimal,
    chrono::{NaiveDate, NaiveDateTime, NaiveTime, Timelike},
    rust_decimal::Decimal,
    std::net::IpAddr,
    uuid::Uuid,
};

#[derive(Debug, Clone)]
pub enum ParamLiteral {
    Literal(Literal),
    TypedString {
        data_type: DataType,
        value: String,
    },
    Interval {
        expr: Box<ParamLiteral>,
        leading_field: Option<DateTimeField>,
        last_field: Option<DateTimeField>,
    },
    Null,
}

impl ParamLiteral {
    #[must_use]
    pub const fn null() -> Self {
        Self::Null
    }

    #[must_use]
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
            ParamLiteral::Null => Expr::Value(Value::Null),
        }
    }
}

fn into_number_literal<T>(value: &T) -> Result<Literal, TranslateError>
where
    T: ToString + ?Sized,
{
    let value_str = value.to_string();
    match value_str.parse::<BigDecimal>() {
        Ok(number) => Ok(Literal::Number(number)),
        Err(_) => Err(TranslateError::InvalidParamLiteral { value: value_str }),
    }
}

pub trait IntoParamLiteral {
    /// Converts the value into a [`ParamLiteral`] so it can be bound as a query parameter.
    ///
    /// # Errors
    ///
    /// Returns an error when the value cannot be represented as a [`ParamLiteral`], such as
    /// non-finite floats (`TranslateError::NonFiniteFloatParameter`) or invalid numeric literals
    /// (`TranslateError::InvalidParamLiteral`).
    fn into_param_literal(self) -> Result<ParamLiteral, TranslateError>;
}

impl IntoParamLiteral for ParamLiteral {
    fn into_param_literal(self) -> Result<ParamLiteral, TranslateError> {
        Ok(self)
    }
}

impl IntoParamLiteral for bool {
    fn into_param_literal(self) -> Result<ParamLiteral, TranslateError> {
        Ok(ParamLiteral::Literal(Literal::Boolean(self)))
    }
}

macro_rules! impl_into_param_literal_for_integer {
    ($($ty:ty),+ $(,)?) => {
        $(
            impl IntoParamLiteral for $ty {
                fn into_param_literal(self) -> Result<ParamLiteral, TranslateError> {
                    into_number_literal(&self).map(ParamLiteral::Literal)
                }
            }
        )+
    };
}

impl_into_param_literal_for_integer!(
    i8, i16, i32, i64, i128, isize, u8, u16, u32, u64, u128, usize
);

impl IntoParamLiteral for f32 {
    fn into_param_literal(self) -> Result<ParamLiteral, TranslateError> {
        if !self.is_finite() {
            return Err(TranslateError::NonFiniteFloatParameter {
                value: self.to_string(),
            });
        }

        into_number_literal(&self).map(ParamLiteral::Literal)
    }
}

impl IntoParamLiteral for f64 {
    fn into_param_literal(self) -> Result<ParamLiteral, TranslateError> {
        if !self.is_finite() {
            return Err(TranslateError::NonFiniteFloatParameter {
                value: self.to_string(),
            });
        }

        into_number_literal(&self).map(ParamLiteral::Literal)
    }
}

impl IntoParamLiteral for Decimal {
    fn into_param_literal(self) -> Result<ParamLiteral, TranslateError> {
        into_number_literal(&self).map(ParamLiteral::Literal)
    }
}

impl IntoParamLiteral for String {
    fn into_param_literal(self) -> Result<ParamLiteral, TranslateError> {
        Ok(ParamLiteral::Literal(Literal::QuotedString(self)))
    }
}

impl IntoParamLiteral for &str {
    fn into_param_literal(self) -> Result<ParamLiteral, TranslateError> {
        self.to_owned().into_param_literal()
    }
}

impl IntoParamLiteral for Vec<u8> {
    fn into_param_literal(self) -> Result<ParamLiteral, TranslateError> {
        Ok(ParamLiteral::Literal(Literal::HexString(hex::encode(self))))
    }
}

impl IntoParamLiteral for &[u8] {
    fn into_param_literal(self) -> Result<ParamLiteral, TranslateError> {
        Ok(ParamLiteral::Literal(Literal::HexString(hex::encode(self))))
    }
}

impl IntoParamLiteral for IpAddr {
    fn into_param_literal(self) -> Result<ParamLiteral, TranslateError> {
        Ok(ParamLiteral::Literal(Literal::QuotedString(
            self.to_string(),
        )))
    }
}

impl IntoParamLiteral for NaiveDate {
    fn into_param_literal(self) -> Result<ParamLiteral, TranslateError> {
        Ok(ParamLiteral::TypedString {
            data_type: DataType::Date,
            value: self.to_string(),
        })
    }
}

impl IntoParamLiteral for NaiveTime {
    fn into_param_literal(self) -> Result<ParamLiteral, TranslateError> {
        Ok(ParamLiteral::TypedString {
            data_type: DataType::Time,
            value: self.to_string(),
        })
    }
}

impl IntoParamLiteral for NaiveDateTime {
    fn into_param_literal(self) -> Result<ParamLiteral, TranslateError> {
        let value = if self.time().nanosecond() == 0 {
            self.format("%Y-%m-%d %H:%M:%S").to_string()
        } else {
            self.format("%Y-%m-%d %H:%M:%S%.f").to_string()
        };

        Ok(ParamLiteral::TypedString {
            data_type: DataType::Timestamp,
            value,
        })
    }
}

impl IntoParamLiteral for Uuid {
    fn into_param_literal(self) -> Result<ParamLiteral, TranslateError> {
        Ok(ParamLiteral::Literal(Literal::QuotedString(
            self.hyphenated().to_string(),
        )))
    }
}

impl IntoParamLiteral for Point {
    fn into_param_literal(self) -> Result<ParamLiteral, TranslateError> {
        Ok(ParamLiteral::Literal(Literal::QuotedString(
            self.to_string(),
        )))
    }
}

impl IntoParamLiteral for Interval {
    fn into_param_literal(self) -> Result<ParamLiteral, TranslateError> {
        match self {
            Interval::Month(months) => Ok(ParamLiteral::Interval {
                expr: Box::new(ParamLiteral::Literal(into_number_literal(&months)?)),
                leading_field: Some(DateTimeField::Month),
                last_field: None,
            }),
            Interval::Microsecond(micros) => Ok(ParamLiteral::Interval {
                expr: Box::new(ParamLiteral::Literal(into_number_literal(&Decimal::new(
                    micros, 6,
                ))?)),
                leading_field: Some(DateTimeField::Second),
                last_field: None,
            }),
        }
    }
}

impl<T> IntoParamLiteral for Option<T>
where
    T: IntoParamLiteral,
{
    fn into_param_literal(self) -> Result<ParamLiteral, TranslateError> {
        match self {
            Some(value) => value.into_param_literal(),
            None => Ok(ParamLiteral::null()),
        }
    }
}

#[macro_export]
macro_rules! params {
    ($($expr:expr),* $(,)?) => {
        vec![
            $(
                $crate::translate::IntoParamLiteral::into_param_literal($expr)
                    .expect("failed to convert parameter literal")
            ),*
        ]
    };
}

#[macro_export]
macro_rules! try_params {
    ($($expr:expr),* $(,)?) => {{
        let result = vec![
            $(
                $crate::translate::IntoParamLiteral::into_param_literal($expr)
            ),*
        ]
        .into_iter()
        .collect::<::core::result::Result<Vec<_>, _>>();

        result
    }};
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            ast::{Expr, Literal},
            data::Point,
        },
        bigdecimal::BigDecimal,
        chrono::{NaiveDate, NaiveTime},
        rust_decimal::Decimal,
        std::{net::IpAddr, str::FromStr},
        uuid::Uuid,
    };

    use std::fmt::{self, Display};

    #[derive(Debug)]
    struct BadNumber;

    impl Display for BadNumber {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "not-a-number")
        }
    }

    impl IntoParamLiteral for BadNumber {
        fn into_param_literal(self) -> Result<ParamLiteral, TranslateError> {
            into_number_literal(&self).map(ParamLiteral::Literal)
        }
    }

    #[test]
    fn accepts_param_literal() {
        let literal = ParamLiteral::null();
        let converted = literal.clone().into_param_literal().unwrap();
        assert!(matches!(
            (literal, converted),
            (ParamLiteral::Null, ParamLiteral::Null)
        ));
    }

    #[test]
    fn converts_basic_literals() {
        let literal = true.into_param_literal().unwrap().into_expr();
        assert_eq!(literal, Expr::Literal(Literal::Boolean(true)));

        let literal = 42_i64.into_param_literal().unwrap().into_expr();
        assert_eq!(literal, Expr::Literal(Literal::Number(42.into())));

        let literal = "glue".into_param_literal().unwrap().into_expr();
        assert_eq!(
            literal,
            Expr::Literal(Literal::QuotedString("glue".to_owned()))
        );

        let literal = String::from("owned").into_param_literal().unwrap();
        assert_eq!(
            literal.into_expr(),
            Expr::Literal(Literal::QuotedString("owned".to_owned()))
        );

        let literal = 3_i16.into_param_literal().unwrap().into_expr();
        assert_eq!(literal, Expr::Literal(Literal::Number(3.into())));

        let literal = 7_u32.into_param_literal().unwrap().into_expr();
        assert_eq!(literal, Expr::Literal(Literal::Number(7.into())));
    }

    #[test]
    fn converts_typed_literals() {
        let date = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
        let expr = date.into_param_literal().unwrap().into_expr();
        assert_eq!(
            expr,
            Expr::TypedString {
                data_type: DataType::Date,
                value: "2024-01-15".to_owned(),
            }
        );

        let time = NaiveTime::from_hms_opt(9, 45, 30).unwrap();
        let expr = time.into_param_literal().unwrap().into_expr();
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
        let expr = timestamp.into_param_literal().unwrap().into_expr();
        assert_eq!(
            expr,
            Expr::TypedString {
                data_type: DataType::Timestamp,
                value: "2024-01-15 12:00:00".to_owned(),
            }
        );

        let timestamp_with_fraction = NaiveDate::from_ymd_opt(2024, 1, 15)
            .unwrap()
            .and_hms_micro_opt(12, 0, 0, 123_456)
            .unwrap();
        let expr = timestamp_with_fraction
            .into_param_literal()
            .unwrap()
            .into_expr();
        assert_eq!(
            expr,
            Expr::TypedString {
                data_type: DataType::Timestamp,
                value: "2024-01-15 12:00:00.123456".to_owned(),
            }
        );
    }

    #[test]
    fn converts_interval() {
        let expr = Interval::Month(2).into_param_literal().unwrap().into_expr();
        assert_eq!(
            expr,
            Expr::Interval {
                expr: Box::new(Expr::Literal(Literal::Number(2.into()))),
                leading_field: Some(DateTimeField::Month),
                last_field: None,
            }
        );

        let expr = Interval::Microsecond(1_500_000)
            .into_param_literal()
            .unwrap()
            .into_expr();
        assert_eq!(
            expr,
            Expr::Interval {
                expr: Box::new(Expr::Literal(Literal::Number(
                    BigDecimal::from_str("1.5").unwrap(),
                ))),
                leading_field: Some(DateTimeField::Second),
                last_field: None,
            }
        );
    }

    #[test]
    fn converts_option() {
        let expr = Some(1_i32).into_param_literal().unwrap().into_expr();
        assert_eq!(expr, Expr::Literal(Literal::Number(1.into())));

        let expr = (None::<i32>).into_param_literal().unwrap().into_expr();
        assert_eq!(expr, Expr::Value(Value::Null));
    }

    #[test]
    fn converts_scalars_and_structs() {
        let expr = 1.25_f64.into_param_literal().unwrap().into_expr();
        assert_eq!(
            expr,
            Expr::Literal(Literal::Number(BigDecimal::from_str("1.25").unwrap()))
        );

        let non_finite_f32 = f32::NAN.into_param_literal();
        assert!(matches!(
            non_finite_f32,
            Err(TranslateError::NonFiniteFloatParameter { ref value }) if value == "NaN"
        ));

        let non_finite_f64 = f64::INFINITY.into_param_literal();
        assert!(matches!(
            non_finite_f64,
            Err(TranslateError::NonFiniteFloatParameter { ref value }) if value == "inf"
        ));

        let expr = 1.5_f32.into_param_literal().unwrap().into_expr();
        assert_eq!(
            expr,
            Expr::Literal(Literal::Number(BigDecimal::from_str("1.5").unwrap()))
        );

        let expr = Decimal::new(345, 2)
            .into_param_literal()
            .unwrap()
            .into_expr();
        assert_eq!(
            expr,
            Expr::Literal(Literal::Number(BigDecimal::from_str("3.45").unwrap()))
        );

        let expr = vec![0x12_u8, 0xAB]
            .into_param_literal()
            .unwrap()
            .into_expr();
        assert_eq!(expr, Expr::Literal(Literal::HexString("12ab".to_owned())));

        let bytes = [0xCD_u8, 0xEF];
        let expr = bytes.as_slice().into_param_literal().unwrap().into_expr();
        assert_eq!(expr, Expr::Literal(Literal::HexString("cdef".to_owned())));

        let ip = IpAddr::from_str("127.0.0.1").unwrap();
        let expr = ip.into_param_literal().unwrap().into_expr();
        assert_eq!(
            expr,
            Expr::Literal(Literal::QuotedString("127.0.0.1".to_owned()))
        );

        let uuid = Uuid::parse_str("123e4567-e89b-12d3-a456-426614174000").unwrap();
        let expr = uuid.into_param_literal().unwrap().into_expr();
        assert_eq!(
            expr,
            Expr::Literal(Literal::QuotedString(
                "123e4567-e89b-12d3-a456-426614174000".to_owned()
            ))
        );

        let point = Point::new(1.0, 2.0);
        let expr = point.into_param_literal().unwrap().into_expr();
        assert_eq!(
            expr,
            Expr::Literal(Literal::QuotedString("POINT(1 2)".to_owned()))
        );

        let invalid = BadNumber.into_param_literal();
        assert!(matches!(
            invalid,
            Err(TranslateError::InvalidParamLiteral { ref value }) if value == "not-a-number"
        ));
    }

    #[test]
    fn params_macro_collects_literals() {
        let params = crate::params![1_i64, "Glue", Some(false), None::<i32>];

        assert_eq!(params.len(), 4);

        assert_eq!(
            params[0].clone().into_expr(),
            Expr::Literal(Literal::Number(1.into()))
        );
        assert_eq!(
            params[1].clone().into_expr(),
            Expr::Literal(Literal::QuotedString("Glue".to_owned()))
        );
        assert_eq!(
            params[2].clone().into_expr(),
            Expr::Literal(Literal::Boolean(false))
        );
        assert_eq!(params[3].clone().into_expr(), Expr::Value(Value::Null));
    }

    #[test]
    fn try_params_macro_propagates_errors() {
        let ok = crate::try_params![1_i64, "Glue"].expect("valid parameters should succeed");
        assert_eq!(ok.len(), 2);

        let err = crate::try_params![1_f64, f64::INFINITY];
        assert!(matches!(
            err,
            Err(TranslateError::NonFiniteFloatParameter { ref value }) if value == "inf"
        ));
    }
}
