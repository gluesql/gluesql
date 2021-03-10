use {
    crate::result::Result,
    boolinator::Boolinator,
    serde::{Deserialize, Serialize},
    sqlparser::ast::{DataType, Expr, Ident, Value as AstValue},
    std::{cmp::Ordering, fmt::Debug},
};

mod ast_value;
mod error;
mod group_key;
mod unique_key;

pub use ast_value::TryFromLiteral;
pub use error::ValueError;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Value {
    Bool(bool),
    I64(i64),
    F64(f64),
    Str(String),
    OptBool(Option<bool>),
    OptI64(Option<i64>),
    OptF64(Option<f64>),
    OptStr(Option<String>),
    Null,
}

impl PartialEq<Value> for Value {
    fn eq(&self, other: &Value) -> bool {
        match (self, other) {
            (Value::Bool(l), Value::Bool(r))
            | (Value::OptBool(Some(l)), Value::Bool(r))
            | (Value::Bool(l), Value::OptBool(Some(r)))
            | (Value::OptBool(Some(l)), Value::OptBool(Some(r))) => l == r,
            (Value::I64(l), Value::I64(r))
            | (Value::OptI64(Some(l)), Value::I64(r))
            | (Value::I64(l), Value::OptI64(Some(r)))
            | (Value::OptI64(Some(l)), Value::OptI64(Some(r))) => l == r,
            (Value::F64(l), Value::F64(r))
            | (Value::OptF64(Some(l)), Value::F64(r))
            | (Value::F64(l), Value::OptF64(Some(r)))
            | (Value::OptF64(Some(l)), Value::OptF64(Some(r))) => l == r,
            (Value::Str(l), Value::Str(r))
            | (Value::OptStr(Some(l)), Value::Str(r))
            | (Value::Str(l), Value::OptStr(Some(r)))
            | (Value::OptStr(Some(l)), Value::OptStr(Some(r))) => l == r,
            (Value::OptBool(None), Value::OptBool(None))
            | (Value::OptI64(None), Value::OptI64(None))
            | (Value::OptF64(None), Value::OptF64(None))
            | (Value::OptStr(None), Value::OptStr(None))
            | (Value::Null, Value::Null) => true,
            _ => false,
        }
    }
}

impl PartialOrd<Value> for Value {
    fn partial_cmp(&self, other: &Value) -> Option<Ordering> {
        match (self, other) {
            (Value::Bool(l), Value::Bool(r))
            | (Value::OptBool(Some(l)), Value::Bool(r))
            | (Value::Bool(l), Value::OptBool(Some(r)))
            | (Value::OptBool(Some(l)), Value::OptBool(Some(r))) => Some(l.cmp(r)),
            (Value::I64(l), Value::I64(r))
            | (Value::OptI64(Some(l)), Value::I64(r))
            | (Value::I64(l), Value::OptI64(Some(r)))
            | (Value::OptI64(Some(l)), Value::OptI64(Some(r))) => Some(l.cmp(r)),
            (Value::I64(l), Value::F64(r))
            | (Value::OptI64(Some(l)), Value::F64(r))
            | (Value::I64(l), Value::OptF64(Some(r)))
            | (Value::OptI64(Some(l)), Value::OptF64(Some(r))) => (*l as f64).partial_cmp(r),
            (Value::F64(l), Value::F64(r))
            | (Value::OptF64(Some(l)), Value::F64(r))
            | (Value::F64(l), Value::OptF64(Some(r)))
            | (Value::OptF64(Some(l)), Value::OptF64(Some(r))) => l.partial_cmp(r),
            (Value::F64(l), Value::I64(r))
            | (Value::OptF64(Some(l)), Value::I64(r))
            | (Value::F64(l), Value::OptI64(Some(r)))
            | (Value::OptF64(Some(l)), Value::OptI64(Some(r))) => l.partial_cmp(&(*r as f64)),
            (Value::Str(l), Value::Str(r))
            | (Value::OptStr(Some(l)), Value::Str(r))
            | (Value::Str(l), Value::OptStr(Some(r)))
            | (Value::OptStr(Some(l)), Value::OptStr(Some(r))) => Some(l.cmp(r)),
            _ => None,
        }
    }
}

trait BoolToValue: Sized {
    fn into_value(self, v1: Value, v2: Value) -> Value;
}

impl BoolToValue for bool {
    #[inline]
    fn into_value(self, v1: Value, v2: Value) -> Value {
        if self {
            v1
        } else {
            v2
        }
    }
}

impl Value {
    pub fn from_expr(data_type: &DataType, nullable: bool, expr: &Expr) -> Result<Self> {
        match expr {
            Expr::Value(literal) => Value::from_data_type(&data_type, nullable, literal),
            Expr::Identifier(Ident { value, .. }) => Ok(Value::Str(value.clone())),
            _ => Err(ValueError::ExprNotSupported(expr.to_string()).into()),
        }
    }

    pub fn is_same_as_data_type(&self, data_type: &DataType) -> bool {
        matches!(
            (data_type, self),
            (DataType::Boolean, Value::Bool(_))
                | (DataType::Boolean, Value::OptBool(Some(_)))
                | (DataType::Text, Value::Str(_))
                | (DataType::Text, Value::OptStr(Some(_)))
                | (DataType::Int, Value::I64(_))
                | (DataType::Int, Value::OptI64(Some(_)))
                | (DataType::Float(_), Value::F64(_))
                | (DataType::Float(_), Value::OptF64(Some(_)))
                | (_, Value::OptBool(None))
                | (_, Value::OptStr(None))
                | (_, Value::OptI64(None))
                | (_, Value::OptF64(None))
        )
    }

    pub fn from_data_type(
        data_type: &DataType,
        nullable: bool,
        literal: &AstValue,
    ) -> Result<Self> {
        match (data_type, literal) {
            (DataType::Int, AstValue::Number(v, false)) => v
                .parse()
                .map(|v| nullable.into_value(Value::OptI64(Some(v)), Value::I64(v)))
                .map_err(|_| ValueError::FailedToParseNumber.into()),
            (DataType::Float(_), AstValue::Number(v, false)) => v
                .parse()
                .map(|v| nullable.into_value(Value::OptF64(Some(v)), Value::F64(v)))
                .map_err(|_| ValueError::FailedToParseNumber.into()),
            (DataType::Boolean, AstValue::Boolean(v)) => {
                Ok(nullable.into_value(Value::OptBool(Some(*v)), Value::Bool(*v)))
            }
            (DataType::Text, AstValue::SingleQuotedString(v)) => {
                Ok(nullable.into_value(Value::OptStr(Some(v.clone())), Value::Str(v.clone())))
            }
            (DataType::Int, AstValue::Null) => nullable.as_result(
                Value::OptI64(None),
                ValueError::NullValueOnNotNullField.into(),
            ),
            (DataType::Float(_), AstValue::Null) => nullable.as_result(
                Value::OptF64(None),
                ValueError::NullValueOnNotNullField.into(),
            ),
            (DataType::Boolean, AstValue::Null) => nullable.as_result(
                Value::OptBool(None),
                ValueError::NullValueOnNotNullField.into(),
            ),
            (DataType::Text, AstValue::Null) => nullable.as_result(
                Value::OptStr(None),
                ValueError::NullValueOnNotNullField.into(),
            ),
            _ => Err(ValueError::SqlTypeNotSupported.into()),
        }
    }

    pub fn cast(&self, data_type: &DataType) -> Result<Self> {
        match (data_type, self) {
            // Same as
            (DataType::Boolean, Value::Bool(_))
            | (DataType::Boolean, Value::OptBool(_))
            | (DataType::Text, Value::Str(_))
            | (DataType::Text, Value::OptStr(_))
            | (DataType::Int, Value::I64(_))
            | (DataType::Int, Value::OptI64(_))
            | (DataType::Float(_), Value::F64(_))
            | (DataType::Float(_), Value::OptF64(_)) => Ok(self.clone()),

            // Boolean
            (DataType::Boolean, Value::Str(value)) => match value.to_uppercase().as_str() {
                "TRUE" => Ok(Value::Bool(true)),
                "FALSE" => Ok(Value::Bool(false)),
                _ => Err(ValueError::ImpossibleCast.into()),
            },
            (DataType::Boolean, Value::OptStr(Some(value))) => {
                match value.to_uppercase().as_str() {
                    "TRUE" => Ok(Value::OptBool(Some(true))),
                    "FALSE" => Ok(Value::OptBool(Some(false))),
                    _ => Err(ValueError::ImpossibleCast.into()),
                }
            }
            (DataType::Boolean, Value::I64(value)) => match value {
                1 => Ok(Value::Bool(true)),
                0 => Ok(Value::Bool(false)),
                _ => Err(ValueError::ImpossibleCast.into()),
            },
            (DataType::Boolean, Value::OptI64(Some(value))) => match value {
                1 => Ok(Value::OptBool(Some(true))),
                0 => Ok(Value::OptBool(Some(false))),
                _ => Err(ValueError::ImpossibleCast.into()),
            },
            (DataType::Boolean, Value::F64(value)) => {
                if value.eq(&1.0) {
                    Ok(Value::Bool(true))
                } else if value.eq(&0.0) {
                    Ok(Value::Bool(false))
                } else {
                    Err(ValueError::ImpossibleCast.into())
                }
            }
            (DataType::Boolean, Value::OptF64(Some(value))) => {
                if value.eq(&1.0) {
                    Ok(Value::OptBool(Some(true)))
                } else if value.eq(&0.0) {
                    Ok(Value::OptBool(Some(false)))
                } else {
                    Err(ValueError::ImpossibleCast.into())
                }
            }
            (DataType::Boolean, Value::OptI64(None))
            | (DataType::Boolean, Value::OptF64(None))
            | (DataType::Boolean, Value::OptStr(None)) => Ok(Value::OptBool(None)),

            // Integer
            (DataType::Int, Value::Bool(value)) => Ok(Value::I64(if *value { 1 } else { 0 })),
            (DataType::Int, Value::OptBool(Some(value))) => {
                Ok(Value::OptI64(Some(if *value { 1 } else { 0 })))
            }
            (DataType::Int, Value::F64(value)) => Ok(Value::I64(value.trunc() as i64)),
            (DataType::Int, Value::OptF64(Some(value))) => {
                Ok(Value::OptI64(Some(value.trunc() as i64)))
            }
            (DataType::Int, Value::Str(value)) => value
                .parse::<i64>()
                .map(Value::I64)
                .map_err(|_| ValueError::ImpossibleCast.into()),
            (DataType::Int, Value::OptStr(Some(value))) => value
                .parse::<i64>()
                .map(Some)
                .map(Value::OptI64)
                .map_err(|_| ValueError::ImpossibleCast.into()),
            (DataType::Int, Value::OptBool(None))
            | (DataType::Int, Value::OptF64(None))
            | (DataType::Int, Value::OptStr(None)) => Ok(Value::OptI64(None)),

            // Float
            (DataType::Float(_), Value::Bool(value)) => {
                Ok(Value::F64(if *value { 1.0 } else { 0.0 }))
            }
            (DataType::Float(_), Value::OptBool(Some(value))) => {
                Ok(Value::OptF64(Some(if *value { 1.0 } else { 0.0 })))
            }
            (DataType::Float(_), Value::I64(value)) => Ok(Value::F64((*value as f64).trunc())),
            (DataType::Float(_), Value::OptI64(Some(value))) => {
                Ok(Value::OptF64(Some((*value as f64).trunc())))
            }
            (DataType::Float(_), Value::Str(value)) => value
                .parse::<f64>()
                .map(Value::F64)
                .map_err(|_| ValueError::ImpossibleCast.into()),
            (DataType::Float(_), Value::OptStr(Some(value))) => value
                .parse::<f64>()
                .map(Some)
                .map(Value::OptF64)
                .map_err(|_| ValueError::ImpossibleCast.into()),
            (DataType::Float(_), Value::OptBool(None))
            | (DataType::Float(_), Value::OptI64(None))
            | (DataType::Float(_), Value::OptStr(None)) => Ok(Value::OptF64(None)),

            // Text
            (DataType::Text, Value::Bool(value)) => Ok(Value::Str(
                (if *value { "TRUE" } else { "FALSE" }).to_string(),
            )),
            (DataType::Text, Value::OptBool(Some(value))) => Ok(Value::OptStr(Some(
                (if *value { "TRUE" } else { "FALSE" }).to_string(),
            ))),
            (DataType::Text, Value::I64(value)) => Ok(Value::Str(value.to_string())),
            (DataType::Text, Value::OptI64(Some(value))) => {
                Ok(Value::OptStr(Some(value.to_string())))
            }
            (DataType::Text, Value::F64(value)) => Ok(Value::Str(value.to_string())),
            (DataType::Text, Value::OptF64(Some(value))) => {
                Ok(Value::OptStr(Some(value.to_string())))
            }
            (DataType::Text, Value::OptBool(None))
            | (DataType::Text, Value::OptI64(None))
            | (DataType::Text, Value::OptF64(None)) => Ok(Value::OptStr(None)),

            _ => Err(ValueError::UnimplementedCast.into()),
        }
    }

    pub fn clone_by(&self, literal: &AstValue) -> Result<Self> {
        match (self, literal) {
            (Value::I64(_), AstValue::Number(v, false)) => v
                .parse()
                .map(Value::I64)
                .map_err(|_| ValueError::FailedToParseNumber.into()),
            (Value::OptI64(_), AstValue::Number(v, false)) => v
                .parse()
                .map(|v| Value::OptI64(Some(v)))
                .map_err(|_| ValueError::FailedToParseNumber.into()),
            (Value::OptI64(_), AstValue::Null) => Ok(Value::OptI64(None)),
            (Value::F64(_), AstValue::Number(v, false)) => v
                .parse()
                .map(Value::F64)
                .map_err(|_| ValueError::FailedToParseNumber.into()),
            (Value::OptF64(_), AstValue::Number(v, false)) => v
                .parse()
                .map(|v| Value::OptF64(Some(v)))
                .map_err(|_| ValueError::FailedToParseNumber.into()),
            (Value::OptF64(_), AstValue::Null) => Ok(Value::OptF64(None)),
            (Value::Str(_), AstValue::SingleQuotedString(v)) => Ok(Value::Str(v.clone())),
            (Value::OptStr(_), AstValue::SingleQuotedString(v)) => {
                Ok(Value::OptStr(Some(v.clone())))
            }
            (Value::OptStr(_), AstValue::Null) => Ok(Value::OptStr(None)),
            (Value::Bool(_), AstValue::Boolean(v)) => Ok(Value::Bool(*v)),
            (Value::OptBool(_), AstValue::Boolean(v)) => Ok(Value::OptBool(Some(*v))),
            (Value::OptBool(_), AstValue::Null) => Ok(Value::OptBool(None)),
            _ => Err(ValueError::LiteralNotSupported.into()),
        }
    }

    pub fn add(&self, other: &Value) -> Result<Value> {
        use Value::*;

        match (self, other) {
            (I64(a), I64(b)) => Ok(I64(a + b)),
            (I64(a), OptI64(Some(b))) | (OptI64(Some(a)), I64(b)) => Ok(OptI64(Some(a + b))),
            (OptI64(Some(a)), OptI64(Some(b))) => Ok(OptI64(Some(a + b))),
            (F64(a), F64(b)) => Ok(F64(a + b)),
            (F64(a), OptF64(Some(b))) | (OptF64(Some(a)), F64(b)) => Ok(OptF64(Some(a + b))),
            (OptI64(None), OptI64(_))
            | (OptI64(_), OptI64(None))
            | (OptI64(None), I64(_))
            | (I64(_), OptI64(None)) => Ok(OptI64(None)),
            (OptF64(_), OptF64(None))
            | (OptF64(None), OptF64(_))
            | (F64(_), OptF64(None))
            | (OptF64(None), F64(_)) => Ok(OptF64(None)),
            _ => Err(ValueError::AddOnNonNumeric.into()),
        }
    }

    pub fn subtract(&self, other: &Value) -> Result<Value> {
        use Value::*;

        match (self, other) {
            (I64(a), I64(b)) => Ok(I64(a - b)),
            (I64(a), OptI64(Some(b))) | (OptI64(Some(a)), I64(b)) => Ok(OptI64(Some(a - b))),
            (F64(a), F64(b)) => Ok(F64(a - b)),
            (F64(a), OptF64(Some(b))) | (OptF64(Some(a)), F64(b)) => Ok(OptF64(Some(a - b))),
            (OptI64(None), OptI64(_))
            | (OptI64(_), OptI64(None))
            | (OptI64(None), I64(_))
            | (I64(_), OptI64(None)) => Ok(OptI64(None)),
            (OptF64(_), OptF64(None))
            | (OptF64(None), OptF64(_))
            | (F64(_), OptF64(None))
            | (OptF64(None), F64(_)) => Ok(OptF64(None)),
            _ => Err(ValueError::SubtractOnNonNumeric.into()),
        }
    }

    pub fn multiply(&self, other: &Value) -> Result<Value> {
        use Value::*;

        match (self, other) {
            (I64(a), I64(b)) => Ok(I64(a * b)),
            (I64(a), OptI64(Some(b))) | (OptI64(Some(a)), I64(b)) => Ok(OptI64(Some(a * b))),
            (F64(a), F64(b)) => Ok(F64(a * b)),
            (F64(a), OptF64(Some(b))) | (OptF64(Some(a)), F64(b)) => Ok(OptF64(Some(a * b))),
            (OptI64(None), OptI64(_))
            | (OptI64(_), OptI64(None))
            | (OptI64(None), I64(_))
            | (I64(_), OptI64(None)) => Ok(OptI64(None)),
            (OptF64(_), OptF64(None))
            | (OptF64(None), OptF64(_))
            | (F64(_), OptF64(None))
            | (OptF64(None), F64(_)) => Ok(OptF64(None)),
            _ => Err(ValueError::MultiplyOnNonNumeric.into()),
        }
    }

    pub fn divide(&self, other: &Value) -> Result<Value> {
        use Value::*;

        match (self, other) {
            (I64(a), I64(b)) => Ok(I64(a / b)),
            (I64(a), OptI64(Some(b))) | (OptI64(Some(a)), I64(b)) => Ok(OptI64(Some(a / b))),
            (F64(a), F64(b)) => Ok(F64(a / b)),
            (F64(a), OptF64(Some(b))) | (OptF64(Some(a)), F64(b)) => Ok(OptF64(Some(a / b))),
            (OptI64(None), OptI64(_))
            | (OptI64(_), OptI64(None))
            | (OptI64(None), I64(_))
            | (I64(_), OptI64(None)) => Ok(OptI64(None)),
            (OptF64(_), OptF64(None))
            | (OptF64(None), OptF64(_))
            | (F64(_), OptF64(None))
            | (OptF64(None), F64(_)) => Ok(OptF64(None)),
            _ => Err(ValueError::DivideOnNonNumeric.into()),
        }
    }

    pub fn is_some(&self) -> bool {
        use Value::*;

        !matches!(
            self,
            Null | OptBool(None) | OptI64(None) | OptF64(None) | OptStr(None)
        )
    }

    pub fn unary_plus(&self) -> Result<Value> {
        use Value::*;

        match self {
            I64(_) | OptI64(_) | F64(_) | OptF64(_) => Ok(self.clone()),
            _ => Err(ValueError::UnaryPlusOnNonNumeric.into()),
        }
    }

    pub fn unary_minus(&self) -> Result<Value> {
        use Value::*;

        match self {
            I64(a) => Ok(I64(-a)),
            OptI64(Some(a)) => Ok(OptI64(Some(-a))),
            F64(a) => Ok(F64(-a)),
            OptF64(Some(a)) => Ok(OptF64(Some(-a))),
            OptI64(None) => Ok(OptI64(None)),
            OptF64(None) => Ok(OptF64(None)),
            _ => Err(ValueError::UnaryMinusOnNonNumeric.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Value;

    #[test]
    fn eq() {
        assert_eq!(Value::Bool(true), Value::OptBool(Some(true)));
        assert_eq!(Value::OptBool(Some(false)), Value::Bool(false));
        assert_eq!(Value::OptBool(None), Value::OptBool(None));

        assert_eq!(Value::I64(1), Value::OptI64(Some(1)));
        assert_eq!(Value::OptI64(Some(1)), Value::I64(1));
        assert_eq!(Value::OptI64(None), Value::OptI64(None));

        assert_eq!(Value::F64(6.11), Value::OptF64(Some(6.11)));
        assert_eq!(Value::OptF64(Some(6.11)), Value::F64(6.11));
        assert_eq!(Value::OptF64(None), Value::OptF64(None));

        let glue = || "Glue".to_owned();

        assert_eq!(Value::Str(glue()), Value::OptStr(Some(glue())));
        assert_eq!(Value::OptStr(Some(glue())), Value::Str(glue()));
        assert_eq!(Value::OptStr(None), Value::OptStr(None));

        assert_eq!(Value::Null, Value::Null);
    }

    #[test]
    fn cast() {
        use {sqlparser::ast::DataType::*, Value::*};

        macro_rules! cast {
            ($input: expr => $data_type: expr, $output: expr) => {
                assert_eq!($input.cast(&$data_type), Ok($output));
            };
        }

        // Same as
        cast!(Bool(true)                    => Boolean      , Bool(true));
        cast!(OptBool(Some(true))           => Boolean      , OptBool(Some(true)));
        cast!(Str("a".to_owned())           => Text         , Str("a".to_owned()));
        cast!(OptStr(Some("a".to_owned()))  => Text         , OptStr(Some("a".to_owned())));
        cast!(I64(1)                        => Int          , I64(1));
        cast!(OptI64(Some(1))               => Int          , OptI64(Some(1)));
        cast!(F64(1.0)                      => Float(None)  , F64(1.0));
        cast!(OptF64(Some(1.0))             => Float(None)  , OptF64(Some(1.0)));

        // Boolean
        cast!(Str("TRUE".to_owned())            => Boolean, Bool(true));
        cast!(Str("FALSE".to_owned())           => Boolean, Bool(false));
        cast!(OptStr(Some("TRUE".to_owned()))   => Boolean, OptBool(Some(true)));
        cast!(OptStr(Some("FALSE".to_owned()))  => Boolean, OptBool(Some(false)));
        cast!(I64(1)                            => Boolean, Bool(true));
        cast!(I64(0)                            => Boolean, Bool(false));
        cast!(OptI64(Some(1))                   => Boolean, OptBool(Some(true)));
        cast!(OptI64(Some(0))                   => Boolean, OptBool(Some(false)));
        cast!(F64(1.0)                          => Boolean, Bool(true));
        cast!(F64(0.0)                          => Boolean, Bool(false));
        cast!(OptF64(Some(1.0))                 => Boolean, OptBool(Some(true)));
        cast!(OptF64(Some(0.0))                 => Boolean, OptBool(Some(false)));
        cast!(OptI64(None)                      => Boolean, OptBool(None));
        cast!(OptF64(None)                      => Boolean, OptBool(None));
        cast!(OptStr(None)                      => Boolean, OptBool(None));

        // Integer
        cast!(Bool(true)                    => Int, I64(1));
        cast!(Bool(false)                   => Int, I64(0));
        cast!(OptBool(Some(true))           => Int, OptI64(Some(1)));
        cast!(OptBool(Some(false))          => Int, OptI64(Some(0)));
        cast!(F64(1.1)                      => Int, I64(1));
        cast!(OptF64(Some(1.1))             => Int, OptI64(Some(1)));
        cast!(Str("11".to_owned())          => Int, I64(11));
        cast!(OptStr(Some("11".to_owned())) => Int, OptI64(Some(11)));
        cast!(OptBool(None)                 => Int, OptI64(None));
        cast!(OptF64(None)                  => Int, OptI64(None));
        cast!(OptStr(None)                  => Int, OptI64(None));

        // Float
        cast!(Bool(true)                    => Float(None), F64(1.0));
        cast!(Bool(false)                   => Float(None), F64(0.0));
        cast!(OptBool(Some(true))           => Float(None), OptF64(Some(1.0)));
        cast!(OptBool(Some(false))          => Float(None), OptF64(Some(0.0)));
        cast!(I64(1)                        => Float(None), F64(1.0));
        cast!(OptI64(Some(1))               => Float(None), OptF64(Some(1.0)));
        cast!(Str("11".to_owned())          => Float(None), F64(11.0));
        cast!(OptStr(Some("11".to_owned())) => Float(None), OptF64(Some(11.0)));
        cast!(OptBool(None)                 => Float(None), OptF64(None));
        cast!(OptI64(None)                  => Float(None), OptF64(None));
        cast!(OptStr(None)                  => Float(None), OptF64(None));

        // Text
        cast!(Bool(true)            => Text, Str("TRUE".to_owned()));
        cast!(Bool(false)           => Text, Str("FALSE".to_owned()));
        cast!(OptBool(Some(true))   => Text, OptStr(Some("TRUE".to_owned())));
        cast!(OptBool(Some(false))  => Text, OptStr(Some("FALSE".to_owned())));
        cast!(I64(11)               => Text, Str("11".to_owned()));
        cast!(OptI64(Some(11))      => Text, OptStr(Some("11".to_owned())));
        cast!(F64(1.0)              => Text, Str("1".to_owned()));
        cast!(OptF64(Some(1.0))     => Text, OptStr(Some("1".to_owned())));
        cast!(OptBool(None)         => Text, OptStr(None));
        cast!(OptI64(None)          => Text, OptStr(None));
        cast!(OptF64(None)          => Text, OptStr(None));
    }
}
