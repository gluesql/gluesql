use {
    crate::{
        ast::Expr,
        data::{Interval, Point, Value},
    },
    chrono::{NaiveDate, NaiveDateTime, NaiveTime},
    rust_decimal::Decimal,
    std::net::IpAddr,
    uuid::Uuid,
};

#[derive(Debug, Clone)]
pub struct ParamLiteral(Value);

impl ParamLiteral {
    #[must_use]
    pub const fn null() -> Self {
        Self(Value::Null)
    }

    #[must_use]
    pub fn into_expr(self) -> Expr {
        Expr::Value(self.0)
    }
}

pub trait IntoParamLiteral {
    /// Converts the value into a [`ParamLiteral`] so it can be bound as a query parameter.
    fn into_param_literal(self) -> ParamLiteral;
}

impl IntoParamLiteral for ParamLiteral {
    fn into_param_literal(self) -> ParamLiteral {
        self
    }
}

macro_rules! impl_into_param_literal {
    ($($rust_ty:ty => $value_variant:ident),+ $(,)?) => {
        $(
            impl IntoParamLiteral for $rust_ty {
                fn into_param_literal(self) -> ParamLiteral {
                    ParamLiteral(Value::$value_variant(self))
                }
            }
        )+
    };
}

impl_into_param_literal!(
    bool => Bool,
    i8 => I8,
    i16 => I16,
    i32 => I32,
    i64 => I64,
    i128 => I128,
    u8 => U8,
    u16 => U16,
    u32 => U32,
    u64 => U64,
    u128 => U128,
    f32 => F32,
    f64 => F64,
    Decimal => Decimal,
    String => Str,
    Vec<u8> => Bytea,
    IpAddr => Inet,
    NaiveDate => Date,
    NaiveTime => Time,
    NaiveDateTime => Timestamp,
    Point => Point,
    Interval => Interval,
);

// Types that need conversion
impl IntoParamLiteral for isize {
    fn into_param_literal(self) -> ParamLiteral {
        ParamLiteral(Value::I64(self as i64))
    }
}

impl IntoParamLiteral for usize {
    fn into_param_literal(self) -> ParamLiteral {
        ParamLiteral(Value::U64(self as u64))
    }
}

impl IntoParamLiteral for &str {
    fn into_param_literal(self) -> ParamLiteral {
        ParamLiteral(Value::Str(self.to_owned()))
    }
}

impl IntoParamLiteral for &[u8] {
    fn into_param_literal(self) -> ParamLiteral {
        ParamLiteral(Value::Bytea(self.to_vec()))
    }
}

impl IntoParamLiteral for Uuid {
    fn into_param_literal(self) -> ParamLiteral {
        ParamLiteral(Value::Uuid(self.as_u128()))
    }
}

impl<T> IntoParamLiteral for Option<T>
where
    T: IntoParamLiteral,
{
    fn into_param_literal(self) -> ParamLiteral {
        match self {
            Some(value) => value.into_param_literal(),
            None => ParamLiteral::null(),
        }
    }
}

#[macro_export]
macro_rules! params {
    ($($expr:expr),* $(,)?) => {
        vec![
            $(
                $crate::translate::IntoParamLiteral::into_param_literal($expr)
            ),*
        ]
    };
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{ast::Expr, data::Point},
        chrono::{NaiveDate, NaiveTime},
        rust_decimal::Decimal,
        std::{net::IpAddr, str::FromStr},
        uuid::Uuid,
    };

    #[test]
    fn accepts_param_literal() {
        let literal = ParamLiteral::null();
        let converted = literal.clone().into_param_literal();
        assert!(matches!(literal.into_expr(), Expr::Value(Value::Null)));
        assert!(matches!(converted.into_expr(), Expr::Value(Value::Null)));
    }

    #[test]
    fn converts_basic_literals() {
        let expr = true.into_param_literal().into_expr();
        assert_eq!(expr, Expr::Value(Value::Bool(true)));

        let expr = 42_i64.into_param_literal().into_expr();
        assert_eq!(expr, Expr::Value(Value::I64(42)));

        let expr = "glue".into_param_literal().into_expr();
        assert_eq!(expr, Expr::Value(Value::Str("glue".to_owned())));

        let expr = String::from("owned").into_param_literal().into_expr();
        assert_eq!(expr, Expr::Value(Value::Str("owned".to_owned())));

        let expr = 3_i16.into_param_literal().into_expr();
        assert_eq!(expr, Expr::Value(Value::I16(3)));

        let expr = 7_u32.into_param_literal().into_expr();
        assert_eq!(expr, Expr::Value(Value::U32(7)));
    }

    #[test]
    fn converts_typed_literals() {
        let date = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
        let expr = date.into_param_literal().into_expr();
        assert_eq!(expr, Expr::Value(Value::Date(date)));

        let time = NaiveTime::from_hms_opt(9, 45, 30).unwrap();
        let expr = time.into_param_literal().into_expr();
        assert_eq!(expr, Expr::Value(Value::Time(time)));

        let timestamp = NaiveDate::from_ymd_opt(2024, 1, 15)
            .unwrap()
            .and_hms_opt(12, 0, 0)
            .unwrap();
        let expr = timestamp.into_param_literal().into_expr();
        assert_eq!(expr, Expr::Value(Value::Timestamp(timestamp)));

        let timestamp_with_fraction = NaiveDate::from_ymd_opt(2024, 1, 15)
            .unwrap()
            .and_hms_micro_opt(12, 0, 0, 123_456)
            .unwrap();
        let expr = timestamp_with_fraction.into_param_literal().into_expr();
        assert_eq!(expr, Expr::Value(Value::Timestamp(timestamp_with_fraction)));
    }

    #[test]
    fn converts_interval() {
        let expr = Interval::Month(2).into_param_literal().into_expr();
        assert_eq!(expr, Expr::Value(Value::Interval(Interval::Month(2))));

        let expr = Interval::Microsecond(1_500_000)
            .into_param_literal()
            .into_expr();
        assert_eq!(
            expr,
            Expr::Value(Value::Interval(Interval::Microsecond(1_500_000)))
        );
    }

    #[test]
    fn converts_option() {
        let expr = Some(1_i32).into_param_literal().into_expr();
        assert_eq!(expr, Expr::Value(Value::I32(1)));

        let expr = (None::<i32>).into_param_literal().into_expr();
        assert_eq!(expr, Expr::Value(Value::Null));
    }

    #[test]
    fn converts_scalars_and_structs() {
        let expr = 1.25_f64.into_param_literal().into_expr();
        assert_eq!(expr, Expr::Value(Value::F64(1.25)));

        let expr = 1.5_f32.into_param_literal().into_expr();
        assert_eq!(expr, Expr::Value(Value::F32(1.5)));

        // NaN and Infinity are supported
        let expr = f32::NAN.into_param_literal().into_expr();
        assert!(matches!(expr, Expr::Value(Value::F32(v)) if v.is_nan()));

        let expr = f64::INFINITY.into_param_literal().into_expr();
        assert_eq!(expr, Expr::Value(Value::F64(f64::INFINITY)));

        let expr = Decimal::new(345, 2).into_param_literal().into_expr();
        assert_eq!(expr, Expr::Value(Value::Decimal(Decimal::new(345, 2))));

        let expr = vec![0x12_u8, 0xAB].into_param_literal().into_expr();
        assert_eq!(expr, Expr::Value(Value::Bytea(vec![0x12, 0xAB])));

        let bytes = [0xCD_u8, 0xEF];
        let expr = bytes.as_slice().into_param_literal().into_expr();
        assert_eq!(expr, Expr::Value(Value::Bytea(vec![0xCD, 0xEF])));

        let ip = IpAddr::from_str("127.0.0.1").unwrap();
        let expr = ip.into_param_literal().into_expr();
        assert_eq!(expr, Expr::Value(Value::Inet(ip)));

        let uuid = Uuid::parse_str("123e4567-e89b-12d3-a456-426614174000").unwrap();
        let expr = uuid.into_param_literal().into_expr();
        assert_eq!(expr, Expr::Value(Value::Uuid(uuid.as_u128())));

        let point = Point::new(1.0, 2.0);
        let expr = point.into_param_literal().into_expr();
        assert_eq!(expr, Expr::Value(Value::Point(point)));
    }

    #[test]
    fn params_macro_collects_literals() {
        let params = crate::params![1_i64, "Glue", Some(false), None::<i32>];

        assert_eq!(params.len(), 4);

        assert_eq!(params[0].clone().into_expr(), Expr::Value(Value::I64(1)));
        assert_eq!(
            params[1].clone().into_expr(),
            Expr::Value(Value::Str("Glue".to_owned()))
        );
        assert_eq!(
            params[2].clone().into_expr(),
            Expr::Value(Value::Bool(false))
        );
        assert_eq!(params[3].clone().into_expr(), Expr::Value(Value::Null));
    }
}
