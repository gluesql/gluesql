use {
    super::{Interval, Key, StringExt},
    crate::{
        ast::{DataType, DateTimeField},
        data::point::Point,
        result::Result,
    },
    binary_op::TryBinaryOperator,
    chrono::{Datelike, NaiveDate, NaiveDateTime, NaiveTime, Timelike},
    core::ops::Sub,
    rust_decimal::Decimal,
    serde::{Deserialize, Serialize},
    std::{cmp::Ordering, collections::HashMap, fmt::Debug, net::IpAddr},
};

mod binary_op;
mod convert;
mod date;
mod error;
mod expr;
mod json;
mod literal;
mod selector;
mod uuid;

pub use {
    convert::ConvertError,
    error::{NumericBinaryOperator, ValueError},
    json::HashMapJsonExt,
};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Value {
    Bool(bool),
    I8(i8),
    I16(i16),
    I32(i32),
    I64(i64),
    I128(i128),
    U8(u8),
    U16(u16),
    U32(u32),
    U64(u64),
    U128(u128),
    F32(f32),
    F64(f64),
    Decimal(Decimal),
    Str(String),
    Bytea(Vec<u8>),
    Inet(IpAddr),
    Date(NaiveDate),
    Timestamp(NaiveDateTime),
    Time(NaiveTime),
    Interval(Interval),
    Uuid(u128),
    Map(HashMap<String, Value>),
    List(Vec<Value>),
    Point(Point),
    Null,
}

impl Value {
    pub fn evaluate_eq(&self, other: &Value) -> bool {
        match (self, other) {
            (Value::I8(l), _) => l == other,
            (Value::I16(l), _) => l == other,
            (Value::I32(l), _) => l == other,
            (Value::I64(l), _) => l == other,
            (Value::I128(l), _) => l == other,
            (Value::U8(l), _) => l == other,
            (Value::U16(l), _) => l == other,
            (Value::U32(l), _) => l == other,
            (Value::U64(l), _) => l == other,
            (Value::U128(l), _) => l == other,
            (Value::F32(l), _) => l == other,
            (Value::F64(l), _) => l == other,
            (Value::Date(l), Value::Timestamp(r)) => l
                .and_hms_opt(0, 0, 0)
                .map(|date_time| &date_time == r)
                .unwrap_or(false),
            (Value::Timestamp(l), Value::Date(r)) => r
                .and_hms_opt(0, 0, 0)
                .map(|date_time| l == &date_time)
                .unwrap_or(false),
            (Value::Null, Value::Null) => false,
            _ => self == other,
        }
    }

    pub fn evaluate_cmp(&self, other: &Value) -> Option<Ordering> {
        match (self, other) {
            (Value::I8(l), _) => l.partial_cmp(other),
            (Value::I16(l), _) => l.partial_cmp(other),
            (Value::I32(l), _) => l.partial_cmp(other),
            (Value::I64(l), _) => l.partial_cmp(other),
            (Value::I128(l), _) => l.partial_cmp(other),
            (Value::U8(l), _) => l.partial_cmp(other),
            (Value::U16(l), _) => l.partial_cmp(other),
            (Value::U32(l), _) => l.partial_cmp(other),
            (Value::U64(l), _) => l.partial_cmp(other),
            (Value::U128(l), _) => l.partial_cmp(other),
            (Value::F32(l), _) => l.partial_cmp(other),
            (Value::F64(l), _) => l.partial_cmp(other),
            (Value::Decimal(l), Value::Decimal(r)) => Some(l.cmp(r)),
            (Value::Bool(l), Value::Bool(r)) => Some(l.cmp(r)),
            (Value::Str(l), Value::Str(r)) => Some(l.cmp(r)),
            (Value::Bytea(l), Value::Bytea(r)) => Some(l.cmp(r)),
            (Value::Inet(l), Value::Inet(r)) => Some(l.cmp(r)),
            (Value::Date(l), Value::Date(r)) => Some(l.cmp(r)),
            (Value::Date(l), Value::Timestamp(r)) => {
                l.and_hms_opt(0, 0, 0).map(|date_time| date_time.cmp(r))
            }
            (Value::Timestamp(l), Value::Date(r)) => {
                r.and_hms_opt(0, 0, 0).map(|date_time| l.cmp(&date_time))
            }
            (Value::Timestamp(l), Value::Timestamp(r)) => Some(l.cmp(r)),
            (Value::Time(l), Value::Time(r)) => Some(l.cmp(r)),
            (Value::Interval(l), Value::Interval(r)) => l.partial_cmp(r),
            (Value::Uuid(l), Value::Uuid(r)) => Some(l.cmp(r)),
            _ => None,
        }
    }

    pub fn is_zero(&self) -> bool {
        match self {
            Value::I8(v) => *v == 0,
            Value::I16(v) => *v == 0,
            Value::I32(v) => *v == 0,
            Value::I64(v) => *v == 0,
            Value::I128(v) => *v == 0,
            Value::U8(v) => *v == 0,
            Value::U16(v) => *v == 0,
            Value::U32(v) => *v == 0,
            Value::U64(v) => *v == 0,
            Value::U128(v) => *v == 0,
            Value::F32(v) => *v == 0.0,
            Value::F64(v) => *v == 0.0,
            Value::Decimal(v) => *v == Decimal::ZERO,
            _ => false,
        }
    }

    pub fn get_type(&self) -> Option<DataType> {
        match self {
            Value::I8(_) => Some(DataType::Int8),
            Value::I16(_) => Some(DataType::Int16),
            Value::I32(_) => Some(DataType::Int32),
            Value::I64(_) => Some(DataType::Int),
            Value::I128(_) => Some(DataType::Int128),
            Value::U8(_) => Some(DataType::Uint8),
            Value::U16(_) => Some(DataType::Uint16),
            Value::U32(_) => Some(DataType::Uint32),
            Value::U64(_) => Some(DataType::Uint64),
            Value::U128(_) => Some(DataType::Uint128),
            Value::F32(_) => Some(DataType::Float32),
            Value::F64(_) => Some(DataType::Float),
            Value::Decimal(_) => Some(DataType::Decimal),
            Value::Bool(_) => Some(DataType::Boolean),
            Value::Str(_) => Some(DataType::Text),
            Value::Bytea(_) => Some(DataType::Bytea),
            Value::Inet(_) => Some(DataType::Inet),
            Value::Date(_) => Some(DataType::Date),
            Value::Timestamp(_) => Some(DataType::Timestamp),
            Value::Time(_) => Some(DataType::Time),
            Value::Interval(_) => Some(DataType::Interval),
            Value::Uuid(_) => Some(DataType::Uuid),
            Value::Map(_) => Some(DataType::Map),
            Value::List(_) => Some(DataType::List),
            Value::Point(_) => Some(DataType::Point),
            Value::Null => None,
        }
    }

    pub fn validate_type(&self, data_type: &DataType) -> Result<()> {
        let valid = self.get_type().map_or(true, |t| t == *data_type);

        if !valid {
            return Err(ValueError::IncompatibleDataType {
                data_type: data_type.clone(),
                value: self.clone(),
            }
            .into());
        }

        Ok(())
    }

    pub fn validate_null(&self, nullable: bool) -> Result<()> {
        if !nullable && matches!(self, Value::Null) {
            return Err(ValueError::NullValueOnNotNullField.into());
        }

        Ok(())
    }

    pub fn cast(&self, data_type: &DataType) -> Result<Self> {
        match (data_type, self) {
            (DataType::Int8, Value::I8(_))
            | (DataType::Int16, Value::I16(_))
            | (DataType::Int32, Value::I32(_))
            | (DataType::Int, Value::I64(_))
            | (DataType::Int128, Value::I128(_))
            | (DataType::Uint8, Value::U8(_))
            | (DataType::Uint16, Value::U16(_))
            | (DataType::Uint32, Value::U32(_))
            | (DataType::Uint64, Value::U64(_))
            | (DataType::Uint128, Value::U128(_))
            | (DataType::Float32, Value::F32(_))
            | (DataType::Float, Value::F64(_))
            | (DataType::Decimal, Value::Decimal(_))
            | (DataType::Boolean, Value::Bool(_))
            | (DataType::Text, Value::Str(_))
            | (DataType::Bytea, Value::Bytea(_))
            | (DataType::Inet, Value::Inet(_))
            | (DataType::Point, Value::Point(_))
            | (DataType::Date, Value::Date(_))
            | (DataType::Timestamp, Value::Timestamp(_))
            | (DataType::Time, Value::Time(_))
            | (DataType::Interval, Value::Interval(_))
            | (DataType::Uuid, Value::Uuid(_)) => Ok(self.clone()),

            (_, Value::Null) => Ok(Value::Null),

            (DataType::Boolean, value) => Ok(value.try_into().map(Value::Bool)?),
            (DataType::Int8, value) => Ok(value.try_into().map(Value::I8)?),
            (DataType::Int16, value) => Ok(value.try_into().map(Value::I16)?),
            (DataType::Int32, value) => Ok(value.try_into().map(Value::I32)?),
            (DataType::Int, value) => Ok(value.try_into().map(Value::I64)?),
            (DataType::Int128, value) => Ok(value.try_into().map(Value::I128)?),
            (DataType::Uint8, value) => Ok(value.try_into().map(Value::U8)?),
            (DataType::Uint16, value) => Ok(value.try_into().map(Value::U16)?),
            (DataType::Uint32, value) => Ok(value.try_into().map(Value::U32)?),
            (DataType::Uint64, value) => Ok(value.try_into().map(Value::U64)?),
            (DataType::Uint128, value) => Ok(value.try_into().map(Value::U128)?),
            (DataType::Float32, value) => Ok(value.try_into().map(Value::F32)?),
            (DataType::Float, value) => Ok(value.try_into().map(Value::F64)?),
            (DataType::Decimal, value) => Ok(value.try_into().map(Value::Decimal)?),

            (DataType::Text, value) => Ok(Value::Str(value.into())),

            (DataType::Date, value) => Ok(value.try_into().map(Value::Date)?),
            (DataType::Time, value) => Ok(value.try_into().map(Value::Time)?),
            (DataType::Timestamp, value) => Ok(value.try_into().map(Value::Timestamp)?),

            (DataType::Interval, Value::Str(value)) => Interval::parse(value).map(Value::Interval),
            (DataType::Uuid, Value::Str(value)) => uuid::parse_uuid(value).map(Value::Uuid),

            (DataType::Uuid, value) => Ok(value.try_into().map(Value::Uuid)?),
            (DataType::Inet, value) => Ok(value.try_into().map(Value::Inet)?),
            (DataType::Point, value) => Ok(value.try_into().map(Value::Point)?),

            (DataType::Bytea, Value::Str(value)) => hex::decode(value)
                .map_err(|_| ValueError::CastFromHexToByteaFailed(value.clone()).into())
                .map(Value::Bytea),
            (DataType::List, Value::Str(value)) => Self::parse_json_list(value),
            (DataType::Map, Value::Str(value)) => Self::parse_json_map(value),

            _ => Err(ValueError::UnimplementedCast {
                value: self.clone(),
                data_type: data_type.clone(),
            }
            .into()),
        }
    }

    pub fn concat(self, other: Value) -> Value {
        match (self, other) {
            (Value::Null, _) | (_, Value::Null) => Value::Null,
            (Value::List(l), Value::List(r)) => Value::List([l, r].concat()),
            (l, r) => Value::Str(String::from(l) + &String::from(r)),
        }
    }

    pub fn add(&self, other: &Value) -> Result<Value> {
        use Value::*;

        match (self, other) {
            (I8(a), b) => a.try_add(b),
            (I16(a), b) => a.try_add(b),
            (I32(a), b) => a.try_add(b),
            (I64(a), b) => a.try_add(b),
            (I128(a), b) => a.try_add(b),
            (U8(a), b) => a.try_add(b),
            (U16(a), b) => a.try_add(b),
            (U32(a), b) => a.try_add(b),
            (U64(a), b) => a.try_add(b),
            (U128(a), b) => a.try_add(b),
            (F32(a), b) => a.try_add(b),
            (F64(a), b) => a.try_add(b),
            (Decimal(a), b) => a.try_add(b),
            (Date(a), Time(b)) => Ok(Timestamp(NaiveDateTime::new(*a, *b))),
            (Date(a), Interval(b)) => b.add_date(a).map(Timestamp),
            (Timestamp(a), Interval(b)) => b.add_timestamp(a).map(Timestamp),
            (Time(a), Interval(b)) => b.add_time(a).map(Time),
            (Interval(a), Interval(b)) => a.add(b).map(Interval),
            (Null, I8(_))
            | (Null, I16(_))
            | (Null, I32(_))
            | (Null, I64(_))
            | (Null, I128(_))
            | (Null, U8(_))
            | (Null, U16(_))
            | (Null, U32(_))
            | (Null, U64(_))
            | (Null, U128(_))
            | (Null, F32(_))
            | (Null, F64(_))
            | (Null, Decimal(_))
            | (Null, Date(_))
            | (Null, Timestamp(_))
            | (Null, Interval(_))
            | (Date(_), Null)
            | (Timestamp(_), Null)
            | (Time(_), Null)
            | (Interval(_), Null)
            | (Null, Null) => Ok(Null),
            _ => Err(ValueError::NonNumericMathOperation {
                lhs: self.clone(),
                operator: NumericBinaryOperator::Add,
                rhs: other.clone(),
            }
            .into()),
        }
    }

    pub fn subtract(&self, other: &Value) -> Result<Value> {
        use {super::Interval as I, Value::*};

        match (self, other) {
            (I8(a), _) => a.try_subtract(other),
            (I16(a), _) => a.try_subtract(other),
            (I32(a), _) => a.try_subtract(other),
            (I64(a), _) => a.try_subtract(other),
            (I128(a), _) => a.try_subtract(other),
            (U8(a), _) => a.try_subtract(other),
            (U16(a), _) => a.try_subtract(other),
            (U32(a), _) => a.try_subtract(other),
            (U64(a), _) => a.try_subtract(other),
            (U128(a), _) => a.try_subtract(other),
            (F32(a), _) => a.try_subtract(other),
            (F64(a), _) => a.try_subtract(other),
            (Decimal(a), _) => a.try_subtract(other),
            (Date(a), Date(b)) => Ok(Interval(I::days((*a - *b).num_days() as i32))),
            (Date(a), Interval(b)) => b.subtract_from_date(a).map(Timestamp),
            (Timestamp(a), Interval(b)) => b.subtract_from_timestamp(a).map(Timestamp),
            (Timestamp(a), Timestamp(b)) => a
                .sub(*b)
                .num_microseconds()
                .ok_or_else(|| {
                    ValueError::UnreachableIntegerOverflow(format!("{:?} - {:?}", a, b)).into()
                })
                .map(|v| Interval(I::microseconds(v))),
            (Time(a), Time(b)) => a
                .sub(*b)
                .num_microseconds()
                .ok_or_else(|| {
                    ValueError::UnreachableIntegerOverflow(format!("{:?} - {:?}", a, b)).into()
                })
                .map(|v| Interval(I::microseconds(v))),
            (Time(a), Interval(b)) => b.subtract_from_time(a).map(Time),
            (Interval(a), Interval(b)) => a.subtract(b).map(Interval),
            (Null, I8(_))
            | (Null, I16(_))
            | (Null, I32(_))
            | (Null, I64(_))
            | (Null, I128(_))
            | (Null, U8(_))
            | (Null, U16(_))
            | (Null, U32(_))
            | (Null, U64(_))
            | (Null, U128(_))
            | (Null, F32(_))
            | (Null, F64(_))
            | (Null, Decimal(_))
            | (Null, Date(_))
            | (Null, Timestamp(_))
            | (Null, Time(_))
            | (Null, Interval(_))
            | (Date(_), Null)
            | (Timestamp(_), Null)
            | (Time(_), Null)
            | (Interval(_), Null)
            | (Null, Null) => Ok(Null),
            _ => Err(ValueError::NonNumericMathOperation {
                lhs: self.clone(),
                operator: NumericBinaryOperator::Subtract,
                rhs: other.clone(),
            }
            .into()),
        }
    }

    pub fn multiply(&self, other: &Value) -> Result<Value> {
        use Value::*;

        match (self, other) {
            (I8(a), _) => a.try_multiply(other),
            (I16(a), _) => a.try_multiply(other),
            (I32(a), _) => a.try_multiply(other),
            (I64(a), _) => a.try_multiply(other),
            (I128(a), _) => a.try_multiply(other),
            (U8(a), _) => a.try_multiply(other),
            (U16(a), _) => a.try_multiply(other),
            (U32(a), _) => a.try_multiply(other),
            (U64(a), _) => a.try_multiply(other),
            (U128(a), _) => a.try_multiply(other),
            (F32(a), _) => a.try_multiply(other),
            (F64(a), _) => a.try_multiply(other),
            (Decimal(a), _) => a.try_multiply(other),
            (Interval(a), I8(b)) => Ok(Interval(*a * *b)),
            (Interval(a), I16(b)) => Ok(Interval(*a * *b)),
            (Interval(a), I32(b)) => Ok(Interval(*a * *b)),
            (Interval(a), I64(b)) => Ok(Interval(*a * *b)),
            (Interval(a), I128(b)) => Ok(Interval(*a * *b)),
            (Interval(a), F32(b)) => Ok(Interval(*a * *b)),
            (Interval(a), F64(b)) => Ok(Interval(*a * *b)),
            (Null, I8(_))
            | (Null, I16(_))
            | (Null, I32(_))
            | (Null, I64(_))
            | (Null, I128(_))
            | (Null, U8(_))
            | (Null, U16(_))
            | (Null, U32(_))
            | (Null, U64(_))
            | (Null, U128(_))
            | (Null, F32(_))
            | (Null, F64(_))
            | (Null, Decimal(_))
            | (Null, Interval(_))
            | (Interval(_), Null)
            | (Null, Null) => Ok(Null),
            _ => Err(ValueError::NonNumericMathOperation {
                lhs: self.clone(),
                operator: NumericBinaryOperator::Multiply,
                rhs: other.clone(),
            }
            .into()),
        }
    }

    pub fn divide(&self, other: &Value) -> Result<Value> {
        use Value::*;

        if other.is_zero() {
            return Err(ValueError::DivisorShouldNotBeZero.into());
        }

        match (self, other) {
            (I8(a), _) => a.try_divide(other),
            (I16(a), _) => a.try_divide(other),
            (I32(a), _) => a.try_divide(other),
            (I64(a), _) => a.try_divide(other),
            (I128(a), _) => a.try_divide(other),
            (U8(a), _) => a.try_divide(other),
            (U16(a), _) => a.try_divide(other),
            (U32(a), _) => a.try_divide(other),
            (U64(a), _) => a.try_divide(other),
            (U128(a), _) => a.try_divide(other),
            (F32(a), _) => a.try_divide(other),
            (F64(a), _) => a.try_divide(other),
            (Decimal(a), _) => a.try_divide(other),
            (Interval(a), I8(b)) => Ok(Interval(*a / *b)),
            (Interval(a), I16(b)) => Ok(Interval(*a / *b)),
            (Interval(a), I32(b)) => Ok(Interval(*a / *b)),
            (Interval(a), I64(b)) => Ok(Interval(*a / *b)),
            (Interval(a), I128(b)) => Ok(Interval(*a / *b)),
            (Interval(a), U8(b)) => Ok(Interval(*a / *b)),
            (Interval(a), U16(b)) => Ok(Interval(*a / *b)),
            (Interval(a), U32(b)) => Ok(Interval(*a / *b)),
            (Interval(a), U64(b)) => Ok(Interval(*a / *b)),
            (Interval(a), U128(b)) => Ok(Interval(*a / *b)),
            (Interval(a), F32(b)) => Ok(Interval(*a / *b)),
            (Interval(a), F64(b)) => Ok(Interval(*a / *b)),
            (Null, I8(_))
            | (Null, I16(_))
            | (Null, I32(_))
            | (Null, I64(_))
            | (Null, I128(_))
            | (Null, U8(_))
            | (Null, U16(_))
            | (Null, U32(_))
            | (Null, U64(_))
            | (Null, U128(_))
            | (Null, F32(_))
            | (Null, F64(_))
            | (Null, Decimal(_))
            | (Interval(_), Null)
            | (Null, Null) => Ok(Null),
            _ => Err(ValueError::NonNumericMathOperation {
                lhs: self.clone(),
                operator: NumericBinaryOperator::Divide,
                rhs: other.clone(),
            }
            .into()),
        }
    }

    pub fn bitwise_and(&self, other: &Value) -> Result<Value> {
        use Value::*;

        match (self, other) {
            (I8(a), I8(b)) => Ok(I8(a & b)),
            (I16(a), I16(b)) => Ok(I16(a & b)),
            (I32(a), I32(b)) => Ok(I32(a & b)),
            (I64(a), I64(b)) => Ok(I64(a & b)),
            (I128(a), I128(b)) => Ok(I128(a & b)),
            (U8(a), U8(b)) => Ok(U8(a & b)),
            (U16(a), U16(b)) => Ok(U16(a & b)),
            (U32(a), U32(b)) => Ok(U32(a & b)),
            (U64(a), U64(b)) => Ok(U64(a & b)),
            (U128(a), U128(b)) => Ok(U128(a & b)),
            (Null, I8(_))
            | (Null, I16(_))
            | (Null, I32(_))
            | (Null, I64(_))
            | (Null, I128(_))
            | (Null, U8(_))
            | (Null, U16(_))
            | (Null, U32(_))
            | (Null, U64(_))
            | (Null, U128(_))
            | (Null, Null)
            | (I8(_), Null)
            | (I16(_), Null)
            | (I32(_), Null)
            | (I64(_), Null)
            | (I128(_), Null)
            | (U8(_), Null)
            | (U16(_), Null)
            | (U32(_), Null)
            | (U64(_), Null)
            | (U128(_), Null) => Ok(Null),
            _ => Err(ValueError::NonNumericMathOperation {
                lhs: self.clone(),
                rhs: other.clone(),
                operator: NumericBinaryOperator::BitwiseAnd,
            }
            .into()),
        }
    }

    pub fn modulo(&self, other: &Value) -> Result<Value> {
        use Value::*;

        if other.is_zero() {
            return Err(ValueError::DivisorShouldNotBeZero.into());
        }

        match (self, other) {
            (I8(a), _) => a.try_modulo(other),
            (I16(a), _) => a.try_modulo(other),
            (I32(a), _) => a.try_modulo(other),
            (I64(a), _) => a.try_modulo(other),
            (I128(a), _) => a.try_modulo(other),
            (U8(a), _) => a.try_modulo(other),
            (U16(a), _) => a.try_modulo(other),
            (U32(a), _) => a.try_modulo(other),
            (U64(a), _) => a.try_modulo(other),
            (U128(a), _) => a.try_modulo(other),
            (F32(a), _) => a.try_modulo(other),
            (F64(a), _) => a.try_modulo(other),
            (Decimal(a), _) => a.try_modulo(other),
            (Null, I8(_))
            | (Null, I16(_))
            | (Null, I32(_))
            | (Null, I64(_))
            | (Null, I128(_))
            | (Null, U8(_))
            | (Null, U16(_))
            | (Null, U32(_))
            | (Null, U64(_))
            | (Null, U128(_))
            | (Null, F32(_))
            | (Null, F64(_))
            | (Null, Decimal(_))
            | (Null, Null) => Ok(Null),
            _ => Err(ValueError::NonNumericMathOperation {
                lhs: self.clone(),
                operator: NumericBinaryOperator::Modulo,
                rhs: other.clone(),
            }
            .into()),
        }
    }

    pub fn bitwise_shift_left(&self, rhs: &Value) -> Result<Value> {
        use Value::*;

        if *rhs == Null {
            return Ok(Null);
        }
        let rhs = u32::try_from(rhs)?;
        match self {
            I8(lhs) => lhs.checked_shl(rhs).map(I8),
            I16(lhs) => lhs.checked_shl(rhs).map(I16),
            I32(lhs) => lhs.checked_shl(rhs).map(I32),
            I64(lhs) => lhs.checked_shl(rhs).map(I64),
            I128(lhs) => lhs.checked_shl(rhs).map(I128),
            U8(lhs) => lhs.checked_shl(rhs).map(U8),
            U16(lhs) => lhs.checked_shl(rhs).map(U16),
            U32(lhs) => lhs.checked_shl(rhs).map(U32),
            U64(lhs) => lhs.checked_shl(rhs).map(U64),
            U128(lhs) => lhs.checked_shl(rhs).map(U128),
            Null => Some(Null),
            _ => {
                return Err(ValueError::NonNumericMathOperation {
                    lhs: self.clone(),
                    rhs: U32(rhs),
                    operator: NumericBinaryOperator::BitwiseShiftLeft,
                }
                .into());
            }
        }
        .ok_or_else(|| {
            ValueError::BinaryOperationOverflow {
                lhs: self.clone(),
                rhs: U32(rhs),
                operator: NumericBinaryOperator::BitwiseShiftLeft,
            }
            .into()
        })
    }

    pub fn bitwise_shift_right(&self, rhs: &Value) -> Result<Value> {
        use Value::*;

        if *rhs == Null {
            return Ok(Null);
        }
        let rhs = u32::try_from(rhs)?;
        match self {
            I8(lhs) => lhs.checked_shr(rhs).map(I8),
            I16(lhs) => lhs.checked_shr(rhs).map(I16),
            I32(lhs) => lhs.checked_shr(rhs).map(I32),
            I64(lhs) => lhs.checked_shr(rhs).map(I64),
            I128(lhs) => lhs.checked_shr(rhs).map(I128),
            U8(lhs) => lhs.checked_shr(rhs).map(U8),
            U16(lhs) => lhs.checked_shr(rhs).map(U16),
            U32(lhs) => lhs.checked_shr(rhs).map(U32),
            U64(lhs) => lhs.checked_shr(rhs).map(U64),
            U128(lhs) => lhs.checked_shr(rhs).map(U128),
            Null => Some(Null),
            _ => {
                return Err(ValueError::NonNumericMathOperation {
                    lhs: self.clone(),
                    rhs: U32(rhs),
                    operator: NumericBinaryOperator::BitwiseShiftRight,
                }
                .into());
            }
        }
        .ok_or_else(|| {
            ValueError::BinaryOperationOverflow {
                lhs: self.clone(),
                rhs: U32(rhs),
                operator: NumericBinaryOperator::BitwiseShiftRight,
            }
            .into()
        })
    }

    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    pub fn unary_plus(&self) -> Result<Value> {
        use Value::*;

        match self {
            I8(_) | I16(_) | I32(_) | I64(_) | I128(_) | U8(_) | U16(_) | U32(_) | U64(_)
            | U128(_) | F32(_) | F64(_) | Interval(_) | Decimal(_) => Ok(self.clone()),
            Null => Ok(Null),
            _ => Err(ValueError::UnaryPlusOnNonNumeric.into()),
        }
    }

    pub fn unary_minus(&self) -> Result<Value> {
        use Value::*;

        match self {
            I8(a) => Ok(I8(-a)),
            I16(a) => Ok(I16(-a)),
            I32(a) => Ok(I32(-a)),
            I64(a) => Ok(I64(-a)),
            I128(a) => Ok(I128(-a)),
            F32(a) => Ok(F32(-a)),
            F64(a) => Ok(F64(-a)),
            Decimal(a) => Ok(Decimal(-a)),
            Interval(a) => Ok(Interval(a.unary_minus())),
            Null => Ok(Null),
            _ => Err(ValueError::UnaryMinusOnNonNumeric.into()),
        }
    }

    pub fn unary_factorial(&self) -> Result<Value> {
        use Value::*;

        fn factorial_function(a: i128) -> Result<i128> {
            if a.is_negative() {
                return Err(ValueError::FactorialOnNegativeNumeric.into());
            }

            (1_i128..(a + 1_i128))
                .try_fold(1_i128, |mul, x| mul.checked_mul(x))
                .ok_or_else(|| ValueError::FactorialOverflow.into())
        }

        match self {
            I8(a) => factorial_function(*a as i128).map(I128),
            I16(a) => factorial_function(*a as i128).map(I128),
            I32(a) => factorial_function(*a as i128).map(I128),
            I64(a) => factorial_function(*a as i128).map(I128),
            I128(a) => factorial_function(*a).map(I128),
            U8(a) => factorial_function(*a as i128).map(I128),
            U16(a) => factorial_function(*a as i128).map(I128),
            U32(a) => factorial_function(*a as i128).map(I128),
            U64(a) => factorial_function(*a as i128).map(I128),
            U128(a) => factorial_function(*a as i128).map(I128),
            F32(_) => Err(ValueError::FactorialOnNonInteger.into()),
            F64(_) => Err(ValueError::FactorialOnNonInteger.into()),
            Null => Ok(Null),
            _ => Err(ValueError::FactorialOnNonNumeric.into()),
        }
    }

    pub fn unary_bitwise_not(&self) -> Result<Value> {
        use Value::*;

        match self {
            I8(v) => Ok(Value::I8(!v)),
            I16(v) => Ok(Value::I16(!v)),
            I32(v) => Ok(Value::I32(!v)),
            I64(v) => Ok(Value::I64(!v)),
            I128(v) => Ok(Value::I128(!v)),
            U8(v) => Ok(Value::U8(!v)),
            U16(v) => Ok(Value::U16(!v)),
            U32(v) => Ok(Value::U32(!v)),
            U64(v) => Ok(Value::U64(!v)),
            U128(v) => Ok(Value::U128(!v)),
            F32(_) => Err(ValueError::UnaryBitwiseNotOnNonInteger.into()),
            F64(_) => Err(ValueError::UnaryBitwiseNotOnNonInteger.into()),
            Null => Ok(Null),
            _ => Err(ValueError::UnaryBitwiseNotOnNonNumeric.into()),
        }
    }

    pub fn like(&self, other: &Value, case_sensitive: bool) -> Result<Value> {
        use Value::*;

        match (self, other) {
            (Str(a), Str(b)) => a.like(b, case_sensitive).map(Bool),
            _ => Err(ValueError::LikeOnNonString {
                base: self.clone(),
                pattern: other.clone(),
                case_sensitive,
            }
            .into()),
        }
    }

    pub fn extract(&self, date_type: &DateTimeField) -> Result<Value> {
        let value = match (self, date_type) {
            (Value::Date(v), DateTimeField::Year) => v.year().into(),
            (Value::Date(v), DateTimeField::Month) => v.month().into(),
            (Value::Date(v), DateTimeField::Day) => v.day().into(),
            (Value::Time(v), DateTimeField::Hour) => v.hour().into(),
            (Value::Time(v), DateTimeField::Minute) => v.minute().into(),
            (Value::Time(v), DateTimeField::Second) => v.second().into(),
            (Value::Timestamp(v), DateTimeField::Year) => v.year().into(),
            (Value::Timestamp(v), DateTimeField::Month) => v.month().into(),
            (Value::Timestamp(v), DateTimeField::Day) => v.day().into(),
            (Value::Timestamp(v), DateTimeField::Hour) => v.hour().into(),
            (Value::Timestamp(v), DateTimeField::Minute) => v.minute().into(),
            (Value::Timestamp(v), DateTimeField::Second) => v.second().into(),
            (Value::Interval(v), _) => {
                return v.extract(date_type);
            }
            _ => {
                return Err(ValueError::ExtractFormatNotMatched {
                    value: self.clone(),
                    field: *date_type,
                }
                .into())
            }
        };

        Ok(Value::I64(value))
    }

    pub fn sqrt(&self) -> Result<Value> {
        use Value::*;
        match self {
            I8(_) | I16(_) | I64(_) | I128(_) | U8(_) | U16(_) | U32(_) | U64(_) | U128(_)
            | F32(_) | F64(_) => {
                let a: f64 = self.try_into()?;
                Ok(Value::F64(a.sqrt()))
            }
            Null => Ok(Value::Null),
            _ => Err(ValueError::SqrtOnNonNumeric(self.clone()).into()),
        }
    }

    /// Value to Big-Endian for comparison purpose
    pub fn to_cmp_be_bytes(&self) -> Result<Vec<u8>> {
        self.try_into().and_then(|key: Key| key.to_cmp_be_bytes())
    }

    /// # Description
    /// The operation method differs depending on the argument.
    /// 1. If both arguments are String
    ///     - Support only [`Value::Str`] variant
    ///     - Returns the position where the first letter of the substring starts if the string contains a substring.
    ///     - Returns [`Value::I64`] 0 if the string to be found is not found.
    ///     - Returns minimum value [`Value::I64`] 1 when the string is found.
    ///     - Returns [`Value::Null`] if NULL parameter found.
    ///
    /// 2. Other arguments
    ///     - Not Supported Yet.
    ///
    /// # Examples
    /// ```
    /// use gluesql_core::prelude::Value;
    ///
    /// let str1 = Value::Str("ramen".to_owned());
    /// let str2 = Value::Str("men".to_owned());
    ///
    /// assert_eq!(str1.position(&str2), Ok(Value::I64(3)));
    /// assert_eq!(str2.position(&str1), Ok(Value::I64(0)));
    /// assert!(Value::Null.position(&str2).unwrap().is_null());
    /// assert!(str1.position(&Value::Null).unwrap().is_null());
    /// ```
    pub fn position(&self, other: &Value) -> Result<Value> {
        use Value::*;

        match (self, other) {
            (Str(from), Str(sub)) => Ok(I64(str_position(from, sub) as i64)),
            (Null, _) | (_, Null) => Ok(Null),
            _ => Err(ValueError::NonStringParameterInPosition {
                from: self.clone(),
                sub: other.clone(),
            }
            .into()),
        }
    }

    pub fn find_idx(&self, sub_val: &Value, start: &Value) -> Result<Value> {
        let start: i64 = start.try_into()?;
        if start <= 0 {
            return Err(ValueError::NonPositiveIntegerOffsetInFindIdx(start.to_string()).into());
        }
        let from = &String::from(self);
        let sub = &String::from(sub_val);
        let position = str_position(&from[(start - 1) as usize..], sub) as i64;
        let position = match position {
            0 => 0,
            _ => position + start - 1,
        };
        Ok(Value::I64(position))
    }
}

fn str_position(from_str: &str, sub_str: &str) -> usize {
    if from_str.is_empty() || sub_str.is_empty() {
        return 0;
    }
    from_str
        .find(sub_str)
        .map(|position| position + 1)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use {
        super::{Interval, Value::*},
        crate::data::{point::Point, value::uuid::parse_uuid, NumericBinaryOperator, ValueError},
        chrono::{NaiveDate, NaiveTime},
        rust_decimal::Decimal,
        std::{net::IpAddr, str::FromStr},
    };

    fn time(hour: u32, min: u32, sec: u32) -> NaiveTime {
        NaiveTime::from_hms_opt(hour, min, sec).unwrap()
    }

    fn date(year: i32, month: u32, day: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(year, month, day).unwrap()
    }

    #[allow(clippy::eq_op)]
    #[test]
    fn evaluate_eq() {
        use {
            super::Interval,
            chrono::{NaiveDateTime, NaiveTime},
        };
        let decimal = |n: i32| Decimal(n.into());
        let bytea = |v: &str| Bytea(hex::decode(v).unwrap());
        let inet = |v: &str| Inet(IpAddr::from_str(v).unwrap());

        assert_eq!(Null, Null);
        assert!(!Null.evaluate_eq(&Null));
        assert!(Bool(true).evaluate_eq(&Bool(true)));
        assert!(I8(1).evaluate_eq(&I8(1)));
        assert!(I16(1).evaluate_eq(&I16(1)));
        assert!(I32(1).evaluate_eq(&I32(1)));
        assert!(I64(1).evaluate_eq(&I64(1)));
        assert!(I128(1).evaluate_eq(&I128(1)));
        assert!(U8(1).evaluate_eq(&U8(1)));
        assert!(U16(1).evaluate_eq(&U16(1)));
        assert!(U32(1).evaluate_eq(&U32(1)));
        assert!(U64(1).evaluate_eq(&U64(1)));
        assert!(U128(1).evaluate_eq(&U128(1)));
        assert!(I64(1).evaluate_eq(&F64(1.0)));
        assert!(F32(1.0_f32).evaluate_eq(&I64(1)));
        assert!(F32(6.11_f32).evaluate_eq(&F64(6.11)));
        assert!(F64(1.0).evaluate_eq(&I64(1)));
        assert!(F64(6.11).evaluate_eq(&F64(6.11)));
        assert!(Str("Glue".to_owned()).evaluate_eq(&Str("Glue".to_owned())));
        assert!(bytea("1004").evaluate_eq(&bytea("1004")));
        assert!(inet("::1").evaluate_eq(&inet("::1")));
        assert!(Interval(Interval::Month(1)).evaluate_eq(&Interval(Interval::Month(1))));
        assert!(Time(NaiveTime::from_hms_opt(12, 30, 11).unwrap())
            .evaluate_eq(&Time(NaiveTime::from_hms_opt(12, 30, 11).unwrap())));
        assert!(decimal(1).evaluate_eq(&decimal(1)));
        assert!(
            Date("2020-05-01".parse().unwrap()).evaluate_eq(&Date("2020-05-01".parse().unwrap()))
        );
        assert!(
            Timestamp("2020-05-01T00:00:00".parse::<NaiveDateTime>().unwrap()).evaluate_eq(
                &Timestamp("2020-05-01T00:00:00".parse::<NaiveDateTime>().unwrap())
            )
        );
        assert!(
            Uuid(parse_uuid("936DA01F9ABD4d9d80C702AF85C822A8").unwrap()).evaluate_eq(&Uuid(
                parse_uuid("936DA01F9ABD4d9d80C702AF85C822A8").unwrap()
            ))
        );
        assert!(Point(Point::new(1.0, 2.0)).evaluate_eq(&Point(Point::new(1.0, 2.0))));

        let date = Date("2020-05-01".parse().unwrap());
        let timestamp = Timestamp("2020-05-01T00:00:00".parse::<NaiveDateTime>().unwrap());

        assert!(date.evaluate_eq(&timestamp));
        assert!(timestamp.evaluate_eq(&date));
    }

    #[test]
    fn cmp() {
        use {
            chrono::{NaiveDate, NaiveTime},
            std::cmp::Ordering,
        };

        assert_eq!(
            Bool(true).evaluate_cmp(&Bool(false)),
            Some(Ordering::Greater)
        );
        assert_eq!(Bool(true).evaluate_cmp(&Bool(true)), Some(Ordering::Equal));
        assert_eq!(
            Bool(false).evaluate_cmp(&Bool(false)),
            Some(Ordering::Equal)
        );
        assert_eq!(Bool(false).evaluate_cmp(&Bool(true)), Some(Ordering::Less));

        let date = Date(NaiveDate::from_ymd_opt(2020, 5, 1).unwrap());
        let timestamp = Timestamp(
            NaiveDate::from_ymd_opt(2020, 3, 1)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap(),
        );

        assert_eq!(date.evaluate_cmp(&timestamp), Some(Ordering::Greater));
        assert_eq!(timestamp.evaluate_cmp(&date), Some(Ordering::Less));

        assert_eq!(
            Time(NaiveTime::from_hms_opt(23, 0, 1).unwrap())
                .evaluate_cmp(&Time(NaiveTime::from_hms_opt(10, 59, 59).unwrap())),
            Some(Ordering::Greater)
        );
        assert_eq!(
            Interval(Interval::Month(1)).evaluate_cmp(&Interval(Interval::Month(2))),
            Some(Ordering::Less)
        );

        let one = Decimal(rust_decimal::Decimal::ONE);
        let two = Decimal(rust_decimal::Decimal::TWO);
        assert_eq!(one.evaluate_cmp(&two), Some(Ordering::Less));
        assert_eq!(two.evaluate_cmp(&one), Some(Ordering::Greater));

        assert_eq!(
            F32(1.0_f32).evaluate_cmp(&F32(1.0_f32)),
            Some(Ordering::Equal)
        );
        assert_eq!(F64(1.0).evaluate_cmp(&F64(1.0)), Some(Ordering::Equal));

        assert_eq!(
            Interval(Interval::Month(1)).evaluate_cmp(&Interval(Interval::Month(1))),
            Some(Ordering::Equal)
        );

        assert_eq!(
            Uuid(parse_uuid("936DA01F9ABD4d9d80C702AF85C822A8").unwrap()).evaluate_cmp(&Uuid(
                parse_uuid("936DA01F9ABD4d9d80C702AF85C822A8").unwrap()
            )),
            Some(Ordering::Equal)
        );

        assert_eq!(Null.evaluate_cmp(&Null), None);

        let bytea = |v: &str| Bytea(hex::decode(v).unwrap());
        assert_eq!(bytea("12").evaluate_cmp(&bytea("20")), Some(Ordering::Less));
        assert_eq!(
            bytea("9123").evaluate_cmp(&bytea("9122")),
            Some(Ordering::Greater)
        );
        assert_eq!(
            bytea("10").evaluate_cmp(&bytea("10")),
            Some(Ordering::Equal)
        );

        let inet = |v: &str| Inet(IpAddr::from_str(v).unwrap());
        assert_eq!(
            inet("0.0.0.0").evaluate_cmp(&inet("127.0.0.1")),
            Some(Ordering::Less)
        );
        assert_eq!(
            inet("192.168.0.1").evaluate_cmp(&inet("127.0.0.1")),
            Some(Ordering::Greater)
        );
        assert_eq!(
            inet("::1").evaluate_cmp(&inet("::1")),
            Some(Ordering::Equal)
        );
    }

    #[test]
    fn cmp_ints() {
        use std::cmp::Ordering;

        assert_eq!(I8(0).evaluate_cmp(&I8(-1)), Some(Ordering::Greater));
        assert_eq!(I8(0).evaluate_cmp(&I8(0)), Some(Ordering::Equal));
        assert_eq!(I8(0).evaluate_cmp(&I8(1)), Some(Ordering::Less));

        assert_eq!(I16(0).evaluate_cmp(&I8(-1)), Some(Ordering::Greater));
        assert_eq!(I16(0).evaluate_cmp(&I8(0)), Some(Ordering::Equal));
        assert_eq!(I16(0).evaluate_cmp(&I8(1)), Some(Ordering::Less));

        assert_eq!(I32(0).evaluate_cmp(&I8(-1)), Some(Ordering::Greater));
        assert_eq!(I32(0).evaluate_cmp(&I8(0)), Some(Ordering::Equal));
        assert_eq!(I32(0).evaluate_cmp(&I8(1)), Some(Ordering::Less));

        assert_eq!(I64(0).evaluate_cmp(&I8(-1)), Some(Ordering::Greater));
        assert_eq!(I64(0).evaluate_cmp(&I8(0)), Some(Ordering::Equal));
        assert_eq!(I64(0).evaluate_cmp(&I8(1)), Some(Ordering::Less));

        assert_eq!(I128(0).evaluate_cmp(&I8(-1)), Some(Ordering::Greater));
        assert_eq!(I128(0).evaluate_cmp(&I8(0)), Some(Ordering::Equal));
        assert_eq!(I128(0).evaluate_cmp(&I8(1)), Some(Ordering::Less));

        assert_eq!(U8(1).evaluate_cmp(&U8(0)), Some(Ordering::Greater));
        assert_eq!(U8(0).evaluate_cmp(&U8(0)), Some(Ordering::Equal));
        assert_eq!(U8(0).evaluate_cmp(&U8(1)), Some(Ordering::Less));

        assert_eq!(U16(1).evaluate_cmp(&U16(0)), Some(Ordering::Greater));
        assert_eq!(U16(0).evaluate_cmp(&U16(0)), Some(Ordering::Equal));
        assert_eq!(U16(0).evaluate_cmp(&U16(1)), Some(Ordering::Less));

        assert_eq!(U32(1).evaluate_cmp(&U32(0)), Some(Ordering::Greater));
        assert_eq!(U32(0).evaluate_cmp(&U32(0)), Some(Ordering::Equal));
        assert_eq!(U32(0).evaluate_cmp(&U32(1)), Some(Ordering::Less));

        assert_eq!(U64(1).evaluate_cmp(&U64(0)), Some(Ordering::Greater));
        assert_eq!(U64(0).evaluate_cmp(&U64(0)), Some(Ordering::Equal));
        assert_eq!(U64(0).evaluate_cmp(&U64(1)), Some(Ordering::Less));

        assert_eq!(U128(1).evaluate_cmp(&U128(0)), Some(Ordering::Greater));
        assert_eq!(U128(0).evaluate_cmp(&U128(0)), Some(Ordering::Equal));
        assert_eq!(U128(0).evaluate_cmp(&U128(1)), Some(Ordering::Less));
    }

    #[test]
    fn is_zero() {
        for i in -1..2 {
            assert_eq!(I8(i).is_zero(), i == 0);
            assert_eq!(I16(i.into()).is_zero(), i == 0);
            assert_eq!(I32(i.into()).is_zero(), i == 0);
            assert_eq!(I64(i.into()).is_zero(), i == 0);
            assert_eq!(I128(i.into()).is_zero(), i == 0);
            assert_eq!(F32(i.into()).is_zero(), i == 0);
            assert_eq!(F64(i.into()).is_zero(), i == 0);
            assert_eq!(Decimal(i.into()).is_zero(), i == 0);
        }
        assert!(U8(0).is_zero());
        assert!(!U8(1).is_zero());
        assert!(U16(0).is_zero());
        assert!(!U16(1).is_zero());
        assert!(U32(0).is_zero());
        assert!(!U32(1).is_zero());
        assert!(U64(0).is_zero());
        assert!(!U64(1).is_zero());
        assert!(U128(0).is_zero());
        assert!(!U128(1).is_zero());
    }

    #[test]
    fn arithmetic() {
        use chrono::{NaiveDate, NaiveTime};

        macro_rules! test {
            ($op: ident $a: expr, $b: expr => $c: expr) => {
                assert!($a.$op(&$b).unwrap().evaluate_eq(&$c));
            };
        }

        macro_rules! mon {
            ($n: expr) => {
                Interval(Interval::Month($n))
            };
        }

        let decimal = |n: i32| Decimal(n.into());

        test!(add I8(1),    I8(2)    => I8(3));
        test!(add I8(1),    I16(2)    => I16(3));
        test!(add I8(1),    I32(2)   => I32(3));
        test!(add I8(1),    I64(2)   => I64(3));
        test!(add I8(1),    I128(2)  => I128(3));
        test!(add I8(1),    U8(2)    => I64(3));

        test!(add I16(1),    I8(2)    => I16(3));
        test!(add I16(1),    I16(2)    => I16(3));
        test!(add I16(1),    I32(2)   => I32(3));
        test!(add I16(1),    I64(2)   => I64(3));
        test!(add I16(1),    I128(2)  => I128(3));
        test!(add I16(1),    U8(2)    => I16(3));

        test!(add I32(1),    I8(2)      => I32(3));
        test!(add I32(1),    I16(2)      => I32(3));
        test!(add I32(1),    I32(2)     => I32(3));
        test!(add I32(1),    I64(2)     => I64(3));
        test!(add I32(1),    I128(2)    => I128(3));
        test!(add I32(1),    U8(2)      => I32(3));

        test!(add I64(1),    I8(2)      => I64(3));
        test!(add I64(1),    I16(2)      => I64(3));
        test!(add I64(1),    I32(2)     => I64(3));
        test!(add I64(1),    I64(2)     => I64(3));
        test!(add I64(1),    I128(2)    => I128(3));
        test!(add I64(1),    U8(2)      => I64(3));

        test!(add I128(1),    I8(2)    => I128(3));
        test!(add I128(1),    I16(2)    => I128(3));
        test!(add I128(1),    I32(2)    => I128(3));
        test!(add I128(1),    I64(2)   => I128(3));
        test!(add I128(1),    I128(2)  => I128(3));
        test!(add I128(1),    U8(2)    => I128(3));

        test!(add I8(1),    F64(2.0) => F64(3.0));

        test!(add I32(1),   I8(2)    => I32(3));
        test!(add I32(1),   I16(2)    => I32(3));
        test!(add I32(1),   I32(2)   => I32(3));
        test!(add I32(1),   I64(2)   => I64(3));
        test!(add I32(1),   F32(2.0_f32) => F32(3.0_f32));
        test!(add I32(1),   F64(2.0) => F64(3.0));

        test!(add I64(1),   I8(2)    => I64(3));
        test!(add I64(1),   I16(2)    => I64(3));
        test!(add I64(1),   I32(2)   => I64(3));
        test!(add I64(1),   I64(2)   => I64(3));
        test!(add I64(1),   F32(2.0_f32) => F32(3.0_f32));
        test!(add I64(1),   F64(2.0) => F64(3.0));

        test!(add I128(1),   I8(2)    => I128(3));
        test!(add I128(1),   I16(2)    => I128(3));
        test!(add I128(1),   I32(2)   => I128(3));
        test!(add I128(1),   I64(2)   => I128(3));
        test!(add I128(1),   F32(2.0_f32) => F32(3.0_f32));
        test!(add I128(1),   F64(2.0) => F64(3.0));

        test!(add U8(1),   I8(2)     => I64(3));
        test!(add U8(1),   I16(2)    => I16(3));
        test!(add U8(1),   I32(2)    => I32(3));
        test!(add U8(1),   I64(2)    => I64(3));
        test!(add U8(1),   I128(2)   => I128(3));
        test!(add U8(1),   U8(2)     => U8(3));
        test!(add U8(1),   F32(2.0_f32)  => F32(3.0_f32));
        test!(add U8(1),   F64(2.0)  => F64(3.0));

        test!(add U16(1),   I8(2)     => U16(3));
        test!(add U16(1),   I16(2)    => U16(3));
        test!(add U16(1),   I32(2)    => U16(3));
        test!(add U16(1),   I64(2)    => U16(3));
        test!(add U16(1),   I128(2)   => U16(3));
        test!(add U16(1),   U8(2)     => U16(3));
        test!(add U16(1),   F32(2.0_f32)  => F32(3.0_f32));
        test!(add U16(1),   F64(2.0)  => F64(3.0));

        test!(add U32(1),   I8(2)     => U32(3));
        test!(add U32(1),   I16(2)    => U32(3));
        test!(add U32(1),   I32(2)    => U32(3));
        test!(add U32(1),   I64(2)    => U32(3));
        test!(add U32(1),   I128(2)   => U32(3));
        test!(add U32(1),   U8(2)     => U32(3));
        test!(add U32(1),   U16(2)     => U32(3));
        test!(add U32(1),   U32(2)     => U32(3));
        test!(add U32(1),   F32(2.0_f32)  => F32(3.0_f32));
        test!(add U32(1),   F64(2.0)  => F64(3.0));

        test!(add U64(1),   I8(2)     => U64(3));
        test!(add U64(1),   I16(2)    => U64(3));
        test!(add U64(1),   I32(2)    => U64(3));
        test!(add U64(1),   I64(2)    => U64(3));
        test!(add U64(1),   I128(2)   => U64(3));
        test!(add U64(1),   U8(2)     => U64(3));
        test!(add U64(1),   U16(2)     => U64(3));
        test!(add U64(1),   U32(2)     => U64(3));
        test!(add U64(1),   F32(2.0_f32)  => F32(3.0_f32));
        test!(add U64(1),   F64(2.0)  => F64(3.0));

        test!(add U128(1),   I8(2)     => U128(3));
        test!(add U128(1),   I16(2)    => U128(3));
        test!(add U128(1),   I32(2)    => U128(3));
        test!(add U128(1),   I64(2)    => U128(3));
        test!(add U128(1),   I128(2)   => U128(3));
        test!(add U128(1),   U8(2)     => U128(3));
        test!(add U128(1),   U16(2)     => U128(3));
        test!(add U128(1),   U32(2)     => U128(3));
        test!(add U128(1),   F32(2.0_f32)  => F32(3.0_f32));
        test!(add U128(1),   F64(2.0)  => F64(3.0));

        test!(add F32(1.0_f32), F32(2.0_f32) => F32(3.0_f32));
        test!(add F32(1.0_f32), F64(2.0) => F64(3.0));
        test!(add F32(1.0_f32), I8(2)    => F32(3.0_f32));
        test!(add F32(1.0_f32), I32(2)   => F32(3.0_f32));
        test!(add F32(1.0_f32), I64(2)   => F32(3.0_f32));
        test!(add F32(1.0_f32), U8(2)   => F32(3.0_f32));
        test!(add F32(1.0_f32), U16(2)   => F32(3.0_f32));
        test!(add F32(1.0_f32), U32(2)   => F32(3.0_f32));
        test!(add F32(1.0_f32), U64(2)   => F32(3.0_f32));
        test!(add F32(1.0_f32), U128(2)   => F32(3.0_f32));

        test!(add F64(1.0), F64(2.0) => F64(3.0));
        test!(add F64(1.0), F32(2.0_f32) => F32(3.0_f32));
        test!(add F64(1.0), I8(2)    => F64(3.0));
        test!(add F64(1.0), I32(2)   => F64(3.0));
        test!(add F64(1.0), I64(2)   => F64(3.0));
        test!(add F64(1.0), U8(2)    => F64(3.0));

        test!(add decimal(1), decimal(2) => decimal(3));

        test!(add
            Date(date(2021, 11, 11)),
            mon!(14)
            =>
            Timestamp(date(2023, 1, 11).and_hms_opt(0, 0, 0).unwrap())
        );
        test!(add
            Date(date(2021, 5, 7)),
            Time(time(12, 0, 0))
            =>
            Timestamp(date(2021, 5, 7).and_hms_opt(12, 0, 0).unwrap())
        );
        test!(add
            Timestamp(date(2021, 11, 11).and_hms_opt(0, 0, 0).unwrap()),
            mon!(14)
            =>
            Timestamp(date(2023, 1, 11).and_hms_opt(0, 0, 0).unwrap())
        );
        test!(add
            Time(time(1, 4, 6)),
            Interval(Interval::hours(20))
            =>
            Time(time(21, 4, 6))
        );
        test!(add
            Time(time(23, 10, 0)),
            Interval(Interval::hours(5))
            =>
            Time(time(4, 10, 0))
        );
        test!(add mon!(1),    mon!(2)    => mon!(3));

        test!(subtract I8(3),    I8(2)    => I8(1));
        test!(subtract I8(3),    I16(2)    => I8(1));
        test!(subtract I8(3),    I32(2)   => I32(1));
        test!(subtract I8(3),    I64(2)   => I64(1));
        test!(subtract I8(3),    I128(2)  => I128(1));
        test!(subtract I8(3),    U8(2)  => I64(1));

        test!(subtract I32(3),    I8(2)    => I32(1));
        test!(subtract I32(3),    I16(2)    => I32(1));
        test!(subtract I32(3),    I32(2)   => I32(1));
        test!(subtract I32(3),    I64(2)   => I64(1));
        test!(subtract I32(3),    I128(2)  => I128(1));
        test!(subtract I32(3),    U8(2)  => I32(1));

        test!(subtract I64(3),    I8(2)    => I64(1));
        test!(subtract I64(3),    I16(2)    => I64(1));
        test!(subtract I64(3),    I32(2)   => I64(1));
        test!(subtract I64(3),    I64(2)   => I64(1));
        test!(subtract I64(3),    I128(2)  => I128(1));
        test!(subtract I64(3),    U8(2)    => I64(1));

        test!(subtract I128(3),    I8(2)   => I128(1));
        test!(subtract I128(3),    I16(2)   => I128(1));
        test!(subtract I128(3),    I32(2)  => I128(1));
        test!(subtract I128(3),    I64(2)  => I128(1));
        test!(subtract I128(3),    I128(2) => I128(1));
        test!(subtract I128(3),    U8(2)   => I128(1));

        test!(subtract U8(3),   I8(2)     => I64(1));
        test!(subtract U8(3),   I16(2)    => I16(1));
        test!(subtract U8(3),   I32(2)    => I32(1));
        test!(subtract U8(3),   I64(2)    => I64(1));
        test!(subtract U8(3),   I128(2)   => I128(1));
        test!(subtract U8(3),   U8(2)     => U8(1));
        test!(subtract U8(3),   F32(2.0_f32)  => F32(1.0_f32));
        test!(subtract U8(3),   F64(2.0)  => F64(1.0));

        test!(subtract U16(3),   I8(2)     => U16(1));
        test!(subtract U16(3),   I16(2)    => U16(1));
        test!(subtract U16(3),   I32(2)    => U16(1));
        test!(subtract U16(3),   I64(2)    => U16(1));
        test!(subtract U16(3),   I128(2)   => U16(1));
        test!(subtract U16(3),   U8(2)     => U16(1));
        test!(subtract U16(3),   F32(2.0_f32)  => F32(1.0_f32));
        test!(subtract U16(3),   F64(2.0)  => F64(1.0));

        test!(subtract U32(3),   I8(2)     => U32(1));
        test!(subtract U32(3),   I16(2)    => U32(1));
        test!(subtract U32(3),   I32(2)    => U32(1));
        test!(subtract U32(3),   I64(2)    => U32(1));
        test!(subtract U32(3),   I128(2)   => U32(1));
        test!(subtract U32(3),   U8(2)     => U32(1));
        test!(subtract U32(3),   F32(2.0_f32)  => F32(1.0_f32));
        test!(subtract U32(3),   F64(2.0)  => F64(1.0));

        test!(subtract U64(3),   I8(2)     => U64(1));
        test!(subtract U64(3),   I16(2)    => U64(1));
        test!(subtract U64(3),   I32(2)    => U64(1));
        test!(subtract U64(3),   I64(2)    => U64(1));
        test!(subtract U64(3),   I128(2)   => U64(1));
        test!(subtract U64(3),   U8(2)     => U64(1));
        test!(subtract U64(3),   F32(2.0_f32)  => F32(1.0_f32));
        test!(subtract U64(3),   F64(2.0)  => F64(1.0));

        test!(subtract U128(3),   I8(2)     => U128(1));
        test!(subtract U128(3),   I16(2)    => U128(1));
        test!(subtract U128(3),   I32(2)    => U128(1));
        test!(subtract U128(3),   I64(2)    => U128(1));
        test!(subtract U128(3),   I128(2)   => U128(1));
        test!(subtract U128(3),   U8(2)     => U128(1));
        test!(subtract U128(3),   F32(2.0_f32)  => F32(1.0_f32));
        test!(subtract U128(3),   F64(2.0)  => F64(1.0));

        test!(subtract I8(3),    F32(2.0_f32) => F32(1.0_f32));
        test!(subtract I32(3),   F32(2.0_f32) => F32(1.0_f32));
        test!(subtract I64(3),   F32(2.0_f32) => F32(1.0_f32));
        test!(subtract I128(3),  F32(2.0_f32) => F32(1.0_f32));
        test!(subtract U8(3),    F32(2.0_f32) => F32(1.0_f32));
        test!(subtract U32(3),   F32(2.0_f32) => F32(1.0_f32));
        test!(subtract U64(3),   F32(2.0_f32) => F32(1.0_f32));
        test!(subtract U128(3),  F32(2.0_f32) => F32(1.0_f32));

        test!(subtract I8(3),    F64(2.0) => F64(1.0));
        test!(subtract I32(3),   F64(2.0) => F64(1.0));
        test!(subtract I64(3),   F64(2.0) => F64(1.0));
        test!(subtract I128(3),  F64(2.0) => F64(1.0));

        test!(subtract I32(3),   I8(2)    => I64(1));
        test!(subtract I32(3),   I16(2)    => I64(1));
        test!(subtract I32(3),   I32(2)   => I32(1));
        test!(subtract I32(3),   I64(2)   => I64(1));
        test!(subtract I32(3),   I128(2)  => I128(1));

        test!(subtract I32(3),   F32(2.0_f32) => F32(1.0_f32));
        test!(subtract I32(3),   F64(2.0) => F64(1.0));

        test!(subtract I64(3),   I8(2)    => I64(1));
        test!(subtract I64(3),   I16(2)    => I64(1));
        test!(subtract I64(3),   I32(2)   => I64(1));
        test!(subtract I64(3),   I64(2)   => I64(1));
        test!(subtract I64(3),   I128(2)   => I64(1));
        test!(subtract I64(3),   F32(2.0_f32) => F32(1.0_f32));
        test!(subtract I64(3),   F64(2.0) => F64(1.0));

        test!(subtract F32(3.0_f32), F32(2.0_f32) => F32(1.0_f32));
        test!(subtract F32(3.0_f32), F64(2.0) => F64(1.0));
        test!(subtract F32(3.0_f32), I8(2)    => F32(1.0_f32));
        test!(subtract F32(3.0_f32), I64(2)   => F32(1.0_f32));

        test!(subtract F64(3.0), F32(2.0_f32) => F32(1.0_f32));
        test!(subtract F64(3.0), F64(2.0) => F64(1.0));
        test!(subtract F64(3.0), I8(2)    => F64(1.0));
        test!(subtract F64(3.0), I64(2)   => F64(1.0));
        test!(subtract F64(3.0), U8(2)   => F64(1.0));

        test!(subtract decimal(3), decimal(2) => decimal(1));

        test!(subtract
            Date(NaiveDate::from_ymd_opt(2021, 11, 11).unwrap()),
            Date(NaiveDate::from_ymd_opt(2021, 6, 11).unwrap())
            =>
            Interval(Interval::days(153))
        );
        test!(subtract
            Date(NaiveDate::from_ymd_opt(2021, 1, 1).unwrap()),
            Interval(Interval::days(365))
            =>
            Timestamp(NaiveDate::from_ymd_opt(2020, 1, 2).unwrap().and_hms_opt(0, 0, 0).unwrap())
        );
        test!(subtract
            Timestamp(NaiveDate::from_ymd_opt(2021, 1, 1).unwrap().and_hms_opt(15, 0, 0).unwrap()),
            Timestamp(NaiveDate::from_ymd_opt(2021, 1, 1).unwrap().and_hms_opt(12, 0, 0).unwrap())
            =>
            Interval(Interval::hours(3))
        );
        test!(subtract
            Timestamp(NaiveDate::from_ymd_opt(2021, 1, 1).unwrap().and_hms_opt(0, 3, 0).unwrap()),
            Interval(Interval::days(365))
            =>
            Timestamp(NaiveDate::from_ymd_opt(2020, 1, 2).unwrap().and_hms_opt(0, 3, 0).unwrap())
        );
        test!(subtract
            Time(time(1, 4, 6)),
            Interval(Interval::hours(20))
            =>
            Time(time(5, 4, 6))
        );
        test!(subtract
            Time(time(23, 10, 0)),
            Interval(Interval::hours(5))
            =>
            Time(time(18, 10, 0))
        );
        test!(subtract mon!(1),  mon!(2)  => mon!(-1));

        test!(multiply I8(3),    I8(2)    => I8(6));
        test!(multiply I8(3),    I16(2)    => I8(6));
        test!(multiply I8(3),    I32(2)    => I32(6));
        test!(multiply I8(3),    I64(2)   => I64(6));
        test!(multiply I8(3),    I128(2)  => I128(6));
        test!(multiply I8(3),    U8(2)    => I64(6));

        test!(multiply I64(3),    I8(2)    => I64(6));
        test!(multiply I64(3),    I16(2)    => I64(6));
        test!(multiply I64(3),    I32(2)   => I64(6));
        test!(multiply I64(3),    I64(2)   => I64(6));
        test!(multiply I64(3),    I128(2)  => I128(6));
        test!(multiply I64(3),    U8(2)    => I64(6));

        test!(multiply I128(3),    I8(2)    => I128(6));
        test!(multiply I128(3),    I16(2)    => I128(6));
        test!(multiply I128(3),    I32(2)    => I128(6));
        test!(multiply I128(3),    I64(2)   => I128(6));
        test!(multiply I128(3),    I128(2)  => I128(6));
        test!(multiply I128(3),    U8(2)  => I128(6));

        test!(multiply I8(3),    F32(2.0_f32) => F32(6.0_f32));
        test!(multiply I16(3),    F32(2.0_f32) => F32(6.0_f32));
        test!(multiply I32(3),    F32(2.0_f32) => F32(6.0_f32));
        test!(multiply I64(3),   F32(2.0_f32) => F32(6.0_f32));
        test!(multiply I128(3),    F32(2.0_f32) => F32(6.0_f32));
        test!(multiply I128(3),    U8(2) => I128(6));

        test!(multiply I8(3),    F64(2.0) => F64(6.0));
        test!(multiply I16(3),    F64(2.0) => F64(6.0));
        test!(multiply I32(3),    F64(2.0) => F64(6.0));
        test!(multiply I64(3),   F64(2.0) => F64(6.0));
        test!(multiply I128(3),    F64(2.0) => F64(6.0));
        test!(multiply I128(3),    U8(2) => I128(6));

        test!(multiply U8(3),   I8(2)     => I64(6));
        test!(multiply U8(3),   I16(2)    => I16(6));
        test!(multiply U8(3),   I32(2)    => I32(6));
        test!(multiply U8(3),   I64(2)    => I64(6));
        test!(multiply U8(3),   I128(2)   => I128(6));
        test!(multiply U8(3),   U8(2)     => U8(6));
        test!(multiply U8(3),   F32(2.0_f32)  => F32(6.0_f32));
        test!(multiply U8(3),   F64(2.0)  => F64(6.0));

        test!(multiply U16(3),   I8(2)     => U16(6));
        test!(multiply U16(3),   I16(2)    => U16(6));
        test!(multiply U16(3),   I32(2)    => U16(6));
        test!(multiply U16(3),   I64(2)    => U16(6));
        test!(multiply U16(3),   I128(2)   => U16(6));
        test!(multiply U16(3),   U8(2)     => U16(6));
        test!(multiply U16(3),   F32(2.0_f32)  => F64(6.0));
        test!(multiply U16(3),   F64(2.0)  => F64(6.0));

        test!(multiply U32(3),   I8(2)     => U32(6));
        test!(multiply U32(3),   I16(2)    => U32(6));
        test!(multiply U32(3),   I32(2)    => U32(6));
        test!(multiply U32(3),   I64(2)    => U32(6));
        test!(multiply U32(3),   I128(2)   => U32(6));
        test!(multiply U32(3),   U8(2)     => U32(6));
        test!(multiply U32(3),   F32(2.0_f32)  => F64(6.0));
        test!(multiply U32(3),   F64(2.0)  => F64(6.0));

        test!(multiply U64(3),   I8(2)     => U64(6));
        test!(multiply U64(3),   I16(2)    => U64(6));
        test!(multiply U64(3),   I32(2)    => U64(6));
        test!(multiply U64(3),   I64(2)    => U64(6));
        test!(multiply U64(3),   I128(2)   => U64(6));
        test!(multiply U64(3),   U8(2)     => U64(6));
        test!(multiply U64(3),   F32(2.0_f32)  => F64(6.0));
        test!(multiply U64(3),   F64(2.0)  => F64(6.0));

        test!(multiply U128(3),   I8(2)     => U128(6));
        test!(multiply U128(3),   I16(2)    => U128(6));
        test!(multiply U128(3),   I32(2)    => U128(6));
        test!(multiply U128(3),   I64(2)    => U128(6));
        test!(multiply U128(3),   I128(2)   => U128(6));
        test!(multiply U128(3),   U8(2)     => U128(6));
        test!(multiply U128(3),   F32(2.0_f32)  => F32(6.0_f32));
        test!(multiply U128(3),   F64(2.0)  => F64(6.0));

        test!(multiply F32(3.0_f32), F32(2.0_f32) => F32(6.0_f32));
        test!(multiply F32(3.0_f32), F64(2.0) => F64(6.0));
        test!(multiply F32(3.0_f32), I8(2)    => F32(6.0_f32));
        test!(multiply F32(3.0_f32), I32(2)   => F32(6.0_f32));
        test!(multiply F32(3.0_f32), I64(2)   => F32(6.0_f32));
        test!(multiply F32(3.0_f32), I128(2)  => F32(6.0_f32));
        test!(multiply F32(3.0_f32), U8(2)    => F32(6.0_f32));

        test!(multiply F64(3.0), F64(2.0) => F64(6.0));
        test!(multiply F64(3.0), F32(2.0_f32) => F32(6.0_f32));
        test!(multiply F64(3.0), I8(2)    => F64(6.0));
        test!(multiply F64(3.0), I32(2)   => F64(6.0));
        test!(multiply F64(3.0), I64(2)   => F64(6.0));
        test!(multiply F64(3.0), I128(2)  => F64(6.0));
        test!(multiply F64(3.0), U8(2)    => F64(6.0));

        test!(multiply decimal(3), decimal(2) => decimal(6));

        test!(multiply I8(3),    mon!(3)  => mon!(9));
        test!(multiply I16(3),   mon!(3)  => mon!(9));
        test!(multiply I32(3),   mon!(3)  => mon!(9));
        test!(multiply I64(3),   mon!(3)  => mon!(9));
        test!(multiply I128(3),  mon!(3)  => mon!(9));
        test!(multiply F32(3.0_f32), mon!(3)  => mon!(9));
        test!(multiply F64(3.0), mon!(3)  => mon!(9));
        test!(multiply mon!(3),  I8(2)    => mon!(6));
        test!(multiply mon!(3),  I16(2)   => mon!(6));
        test!(multiply mon!(3),  I32(2)   => mon!(6));
        test!(multiply mon!(3),  I64(2)   => mon!(6));
        test!(multiply mon!(3),  I128(2)  => mon!(6));
        test!(multiply mon!(3),  F32(2.0_f32) => mon!(6));
        test!(multiply mon!(3),  F32(2.0_f32) => mon!(6));
        test!(multiply mon!(3),  F64(2.0) => mon!(6));

        test!(divide I8(0),     I8(5)   => I8(0));
        test!(divide I8(0),     I16(5)   => I8(0));
        test!(divide I8(0),     I32(5)  => I32(0));
        test!(divide I8(0),     I64(5)  => I64(0));
        test!(divide I8(0),     I128(5) => I128(0));
        test!(divide I8(0),     U8(5)   => I64(0));
        assert_eq!(
            I8(5).divide(&I8(0)),
            Err(ValueError::DivisorShouldNotBeZero.into())
        );

        test!(divide I8(6),    I8(2)    => I8(3));
        test!(divide I8(6),    I16(2)    => I8(3));
        test!(divide I8(6),    I32(2)    => I8(3));
        test!(divide I8(6),    I64(2)   => I64(3));
        test!(divide I8(6),    I128(2)  => I128(3));
        test!(divide I8(6),    U8(2)    => I64(3));

        test!(divide I64(6),    I8(2)    => I64(3));
        test!(divide I64(6),    I16(2)    => I64(3));
        test!(divide I64(6),    I32(2)    => I64(3));
        test!(divide I64(6),    I64(2)   => I64(3));
        test!(divide I64(6),    I128(2)  => I128(3));
        test!(divide I64(6),    U8(2)  => I64(3));

        test!(divide I128(6),    I8(2)    => I128(3));
        test!(divide I128(6),    I16(2)    => I128(3));
        test!(divide I128(6),    I32(2)    => I128(3));
        test!(divide I128(6),    I64(2)   => I128(3));
        test!(divide I128(6),    I128(2)  => I128(3));
        test!(divide I128(6),    U8(2)  => I64(3));

        test!(divide I128(6),    I8(2)    => I128(3));
        test!(divide I128(6),    I16(2)    => I128(3));
        test!(divide I128(6),    I32(2)    => I128(3));
        test!(divide I128(6),    I64(2)   => I128(3));
        test!(divide I128(6),    I128(2)  => I128(3));

        test!(divide U8(6),   I8(2)     => I64(3));
        test!(divide U8(6),   I16(2)    => I16(3));
        test!(divide U8(6),   I32(2)    => I32(3));
        test!(divide U8(6),   I64(2)    => I64(3));
        test!(divide U8(6),   I128(2)   => I128(3));
        test!(divide U8(6),   U8(2)     => U8(3));
        test!(divide U8(6),   F32(2.0_f32)  => F64(3.0));
        test!(divide U8(6),   F64(2.0)  => F64(3.0));

        test!(divide U16(6),   I8(2)     => U16(3));
        test!(divide U16(6),   I16(2)    => U16(3));
        test!(divide U16(6),   I32(2)    => U16(3));
        test!(divide U16(6),   I64(2)    => U16(3));
        test!(divide U16(6),   I128(2)   => U16(3));
        test!(divide U16(6),   U8(2)     => U16(3));
        test!(divide U16(6),   F32(2.0_f32)  => F64(3.0));
        test!(divide U16(6),   F64(2.0)  => F64(3.0));

        test!(divide U32(6),   I8(2)     => U32(3));
        test!(divide U32(6),   I16(2)    => U32(3));
        test!(divide U32(6),   I32(2)    => U32(3));
        test!(divide U32(6),   I64(2)    => U32(3));
        test!(divide U32(6),   I128(2)   => U32(3));
        test!(divide U32(6),   U8(2)     => U32(3));
        test!(divide U32(6),   F32(2.0_f32)  => F64(3.0));
        test!(divide U32(6),   F64(2.0)  => F64(3.0));

        test!(divide U64(6),   I8(2)     => U64(3));
        test!(divide U64(6),   I16(2)    => U64(3));
        test!(divide U64(6),   I32(2)    => U64(3));
        test!(divide U64(6),   I64(2)    => U64(3));
        test!(divide U64(6),   I128(2)   => U64(3));
        test!(divide U64(6),   U8(2)     => U64(3));
        test!(divide U64(6),   F32(2.0_f32)  => F64(3.0));
        test!(divide U64(6),   F64(2.0)  => F64(3.0));

        test!(divide U128(6),   I8(2)     => U128(3));
        test!(divide U128(6),   I16(2)    => U128(3));
        test!(divide U128(6),   I32(2)    => U128(3));
        test!(divide U128(6),   I64(2)    => U128(3));
        test!(divide U128(6),   I128(2)   => U128(3));
        test!(divide U128(6),   U8(2)     => U128(3));
        test!(divide U128(6),   F64(2.0)  => F64(3.0));

        test!(divide I8(6),    F64(2.0) => F64(3.0));
        test!(divide I32(6),    F64(2.0) => F64(3.0));
        test!(divide I64(6),   F64(2.0) => F64(3.0));
        test!(divide I128(6),    F64(2.0) => F64(3.0));
        test!(divide F32(6.0_f32),    F64(2.0) => F64(3.0));

        test!(divide I8(6),    F32(2.0_f32) => F32(3.0_f32));
        test!(divide I32(6),    F32(2.0_f32) => F32(3.0_f32));
        test!(divide I64(6),   F32(2.0_f32) => F32(3.0_f32));
        test!(divide I128(6),    F32(2.0_f32) => F32(3.0_f32));
        test!(divide F64(6.0), F32(2.0_f32) => F32(3.0_f32));

        test!(divide F32(6.0_f32), I8(2)    => F32(3.0_f32));
        test!(divide F32(6.0_f32), I16(2)    => F32(3.0_f32));
        test!(divide F32(6.0_f32), I32(2)    => F32(3.0_f32));
        test!(divide F32(6.0_f32), I64(2)   => F32(3.0_f32));
        test!(divide F32(6.0_f32), I128(2)    => F32(3.0_f32));
        test!(divide F64(6.0), F32(2.0_f32) => F32(3.0_f32));

        test!(divide F64(6.0), I8(2)    => F64(3.0));
        test!(divide F64(6.0), I16(2)    => F64(3.0));
        test!(divide F64(6.0), I32(2)    => F64(3.0));
        test!(divide F64(6.0), I64(2)   => F64(3.0));
        test!(divide F64(6.0), I128(2)    => F64(3.0));
        test!(divide F64(6.0), U8(2)    => F64(3.0));
        test!(divide F64(6.0), F32(2.0_f32) => F32(3.0_f32));

        test!(divide mon!(6),  I8(2)    => mon!(3));
        test!(divide mon!(6),  I16(2)    => mon!(3));
        test!(divide mon!(6),  I32(2)    => mon!(3));
        test!(divide mon!(6),  I64(2)   => mon!(3));
        test!(divide mon!(6),  I128(2)    => mon!(3));
        test!(divide mon!(6),  U8(2)    => mon!(3));
        test!(divide mon!(6),  U16(2)    => mon!(3));
        test!(divide mon!(6),  U32(2)    => mon!(3));
        test!(divide mon!(6),  U64(2)    => mon!(3));
        test!(divide mon!(6),  U128(2)    => mon!(3));
        test!(divide mon!(6),  F32(2.0_f32) => mon!(3));
        test!(divide mon!(6),  F64(2.0) => mon!(3));

        test!(modulo I8(6),    I8(4)    => I8(2));
        test!(modulo I8(6),    I16(4)    => I8(2));
        test!(modulo I8(6),    I32(4)    => I8(2));
        test!(modulo I8(6),    I64(4)   => I64(2));
        test!(modulo I8(6),    I128(4)  => I128(2));

        assert_eq!(
            I8(5).modulo(&I8(0)),
            Err(ValueError::DivisorShouldNotBeZero.into())
        );

        test!(modulo I64(6),    I8(4)    => I64(2));
        test!(modulo I64(6),    I16(4)    => I64(2));
        test!(modulo I64(6),    I32(4)   => I64(2));
        test!(modulo I64(6),    I64(4)   => I64(2));
        test!(modulo I64(6),    I128(4)  => I128(2));

        test!(modulo I128(6),    I8(4)    => I128(2));
        test!(modulo I128(6),    I16(4)    => I128(2));
        test!(modulo I128(6),    I32(4)    => I128(2));
        test!(modulo I128(6),    I64(4)   => I128(2));
        test!(modulo I128(6),    I128(4)  => I128(2));

        test!(modulo I8(6),   I8(2)   => I8(0));
        test!(modulo I8(6),   F32(2.0_f32) => F32(0.0_f32));
        test!(modulo I8(6),   F64(2.0) => F64(0.0));
        test!(modulo I32(6),   I32(2)   => I32(0));
        test!(modulo I32(6),   F64(2.0) => F64(0.0));
        test!(modulo I64(6),   I32(2)   => I32(0));
        test!(modulo I64(6),   F64(2.0) => F64(0.0));
        test!(modulo F32(6.0_f32), I64(2)   => F32(0.0_f32));
        test!(modulo F32(6.0_f32), F32(2.0_f32) => F32(0.0_f32));
        test!(modulo F64(6.0), I64(2)   => F64(0.0));
        test!(modulo F64(6.0), F64(2.0) => F64(0.0));
        test!(modulo I128(6),   I8(2)   => I128(0));
        test!(modulo I128(6),   I16(2)   => I128(0));
        test!(modulo I128(6),   I32(2)   => I128(0));
        test!(modulo I128(6),   I64(2)   => I128(0));
        test!(modulo I128(6),   I128(2)   => I128(0));
        test!(modulo I128(6),   F64(2.0) => F64(0.0));
        test!(modulo I128(6),   F32(2.0_f32) => F32(0.0_f32));

        macro_rules! null_test {
            ($op: ident $a: expr, $b: expr) => {
                assert!($a.$op(&$b).unwrap().is_null());
            };
        }

        let date = || Date(NaiveDate::from_ymd_opt(1989, 3, 1).unwrap());
        let time = || Time(NaiveTime::from_hms_opt(6, 1, 1).unwrap());
        let ts = || {
            Timestamp(
                NaiveDate::from_ymd_opt(1989, 1, 1)
                    .unwrap()
                    .and_hms_opt(0, 0, 0)
                    .unwrap(),
            )
        };

        null_test!(add      I8(1),    Null);
        null_test!(add      I16(1),    Null);
        null_test!(add      I32(1),   Null);
        null_test!(add      I64(1),   Null);
        null_test!(add      I128(1),   Null);
        null_test!(add      U8(1),   Null);
        null_test!(add      U16(1),   Null);
        null_test!(add      U32(1),   Null);
        null_test!(add      U64(1),   Null);
        null_test!(add      U128(1),   Null);
        null_test!(add      F32(1.0_f32), Null);
        null_test!(add      F64(1.0), Null);
        null_test!(add      decimal(1), Null);
        null_test!(add      date(),   Null);
        null_test!(add      ts(),     Null);
        null_test!(add      time(),   Null);
        null_test!(add      mon!(1),  Null);
        null_test!(subtract I8(1),    Null);
        null_test!(subtract I16(1),    Null);
        null_test!(subtract I32(1),    Null);
        null_test!(subtract I64(1),   Null);
        null_test!(subtract I128(1),   Null);
        null_test!(subtract U8(1),   Null);
        null_test!(subtract U16(1),   Null);
        null_test!(subtract U32(1),   Null);
        null_test!(subtract U64(1),   Null);
        null_test!(subtract U128(1),   Null);
        null_test!(subtract F32(1.0_f32), Null);
        null_test!(subtract F64(1.0), Null);
        null_test!(subtract decimal(1), Null);
        null_test!(subtract date(),   Null);
        null_test!(subtract ts(),     Null);
        null_test!(subtract time(),   Null);
        null_test!(subtract mon!(1),  Null);
        null_test!(multiply I8(1),    Null);
        null_test!(multiply I16(1),    Null);
        null_test!(multiply I32(1),   Null);
        null_test!(multiply I64(1),   Null);
        null_test!(multiply I128(1),   Null);
        null_test!(multiply U8(1),   Null);
        null_test!(multiply U16(1),   Null);
        null_test!(multiply U32(1),   Null);
        null_test!(multiply U64(1),   Null);
        null_test!(multiply U128(1),   Null);
        null_test!(multiply F32(1.0_f32), Null);
        null_test!(multiply F64(1.0), Null);
        null_test!(multiply decimal(1), Null);
        null_test!(multiply mon!(1),  Null);
        null_test!(divide   I8(1),    Null);
        null_test!(divide   I16(1),    Null);
        null_test!(divide   I32(1),    Null);
        null_test!(divide   I64(1),   Null);
        null_test!(divide   I128(1),   Null);
        null_test!(divide   U8(1),   Null);
        null_test!(divide   U16(1),   Null);
        null_test!(divide   U32(1),   Null);
        null_test!(divide   U64(1),   Null);
        null_test!(divide   U128(1),   Null);
        null_test!(divide   F32(1.0_f32), Null);
        null_test!(divide   F64(1.0), Null);
        null_test!(divide   decimal(1), Null);
        null_test!(divide   mon!(1),  Null);
        null_test!(modulo   I8(1),    Null);
        null_test!(modulo   I16(1),    Null);
        null_test!(modulo   I32(1),    Null);
        null_test!(modulo   I64(1),   Null);
        null_test!(modulo   I128(1),   Null);
        null_test!(modulo   U8(1),   Null);
        null_test!(modulo   U16(1),   Null);
        null_test!(modulo   U32(1),   Null);
        null_test!(modulo   U64(1),   Null);
        null_test!(modulo   U128(1),   Null);
        null_test!(modulo   F32(1.0_f32), Null);
        null_test!(modulo   F64(1.0), Null);
        null_test!(modulo   decimal(1), Null);

        null_test!(add      Null, I8(1));
        null_test!(add      Null, I16(1));
        null_test!(add      Null, I32(1));
        null_test!(add      Null, I64(1));
        null_test!(add      Null, I128(1));
        null_test!(add      Null, U8(1));
        null_test!(add      Null, U16(1));
        null_test!(add      Null, U32(1));
        null_test!(add      Null, U64(1));
        null_test!(add      Null, U128(1));
        null_test!(add      Null, F32(1.0_f32));
        null_test!(add      Null, F64(1.0));
        null_test!(add      Null, decimal(1));
        null_test!(add      Null, mon!(1));
        null_test!(add      Null, date());
        null_test!(add      Null, ts());
        null_test!(subtract Null, I8(1));
        null_test!(subtract Null, I16(1));
        null_test!(subtract Null, I32(1));
        null_test!(subtract Null, I64(1));
        null_test!(subtract Null, I128(1));
        null_test!(subtract Null, U8(1));
        null_test!(subtract Null, U16(1));
        null_test!(subtract Null, U32(1));
        null_test!(subtract Null, U64(1));
        null_test!(subtract Null, U128(1));
        null_test!(subtract Null, F32(1.0_f32));
        null_test!(subtract Null, F64(1.0));
        null_test!(subtract Null, decimal(1));
        null_test!(subtract Null, date());
        null_test!(subtract Null, ts());
        null_test!(subtract Null, time());
        null_test!(subtract Null, mon!(1));
        null_test!(multiply Null, I8(1));
        null_test!(multiply Null, I16(1));
        null_test!(multiply Null, I32(1));
        null_test!(multiply Null, I64(1));
        null_test!(multiply Null, I128(1));
        null_test!(multiply Null, U8(1));
        null_test!(multiply Null, U16(1));
        null_test!(multiply Null, U32(1));
        null_test!(multiply Null, U64(1));
        null_test!(multiply Null, U128(1));
        null_test!(multiply Null, F32(1.0_f32));
        null_test!(multiply Null, F64(1.0));
        null_test!(multiply Null, decimal(1));
        null_test!(divide   Null, I8(1));
        null_test!(divide   Null, I16(1));
        null_test!(divide   Null, I32(1));
        null_test!(divide   Null, I64(1));
        null_test!(divide   Null, I128(1));
        null_test!(divide   Null, U8(1));
        null_test!(divide   Null, U16(1));
        null_test!(divide   Null, U32(1));
        null_test!(divide   Null, U64(1));
        null_test!(divide   Null, U128(1));
        null_test!(divide   Null, F32(1.0_f32));
        null_test!(divide   Null, F64(1.0));
        null_test!(divide   Null, decimal(1));
        null_test!(modulo   Null, I8(1));
        null_test!(modulo   Null, I32(1));
        null_test!(modulo   Null, I64(1));
        null_test!(modulo   Null, I128(1));
        null_test!(modulo   Null, U8(1));
        null_test!(modulo   Null, U16(1));
        null_test!(modulo   Null, U32(1));
        null_test!(modulo   Null, U64(1));
        null_test!(modulo   Null, U128(1));
        null_test!(modulo   Null, F32(1.0_f32));
        null_test!(modulo   Null, F64(1.0));
        null_test!(modulo   Null, decimal(1));

        null_test!(add      Null, Null);
        null_test!(subtract Null, Null);
        null_test!(multiply Null, Null);
        null_test!(divide   Null, Null);
        null_test!(modulo   Null, Null);
    }

    #[test]
    fn bitwise_shift_left() {
        use {super::convert::ConvertError, crate::ast::DataType};

        macro_rules! test {
            ($op: ident $a: expr, $b: expr => $c: expr) => {
                assert!($a.$op(&$b).unwrap().evaluate_eq(&$c));
            };
        }

        macro_rules! mon {
            ($n: expr) => {
                Interval(Interval::Month($n))
            };
        }

        // operation result test
        test!(bitwise_shift_left I8(1),     I64(2) => I8(4));
        test!(bitwise_shift_left I16(1),    I64(2) => I16(4));
        test!(bitwise_shift_left I32(1),    I64(2) => I32(4));
        test!(bitwise_shift_left I64(1),    I64(2) => I64(4));
        test!(bitwise_shift_left I128(1),   I64(2) => I128(4));
        test!(bitwise_shift_left U8(1),     I64(2) => U8(4));
        test!(bitwise_shift_left U16(1),    I64(2) => U16(4));
        test!(bitwise_shift_left U32(1),    I64(2) => U32(4));
        test!(bitwise_shift_left U64(1),    I64(2) => U64(4));
        test!(bitwise_shift_left U128(1),   I64(2) => U128(4));
        test!(bitwise_shift_left I8(1),     U32(2) => I8(4));
        test!(bitwise_shift_left I16(1),    U32(2) => I16(4));
        test!(bitwise_shift_left I32(1),    U32(2) => I32(4));
        test!(bitwise_shift_left I64(1),    U32(2) => I64(4));
        test!(bitwise_shift_left I128(1),   U32(2) => I128(4));
        test!(bitwise_shift_left U8(1),     U32(2) => U8(4));
        test!(bitwise_shift_left U16(1),    U32(2) => U16(4));
        test!(bitwise_shift_left U32(1),    U32(2) => U32(4));
        test!(bitwise_shift_left U64(1),    U32(2) => U64(4));
        test!(bitwise_shift_left U128(1),   U32(2) => U128(4));

        //overflow test
        assert_eq!(
            I8(1).bitwise_shift_left(&I64(100)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: I8(1),
                rhs: U32(100),
                operator: NumericBinaryOperator::BitwiseShiftLeft
            }
            .into())
        );
        assert_eq!(
            I16(1).bitwise_shift_left(&I64(100)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: I16(1),
                rhs: U32(100),
                operator: NumericBinaryOperator::BitwiseShiftLeft
            }
            .into())
        );
        assert_eq!(
            I32(1).bitwise_shift_left(&I64(100)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: I32(1),
                rhs: U32(100),
                operator: NumericBinaryOperator::BitwiseShiftLeft
            }
            .into())
        );
        assert_eq!(
            I64(1).bitwise_shift_left(&I64(100)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: I64(1),
                rhs: U32(100),
                operator: NumericBinaryOperator::BitwiseShiftLeft
            }
            .into())
        );
        assert_eq!(
            I128(1).bitwise_shift_left(&I64(150)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: I128(1),
                rhs: U32(150),
                operator: NumericBinaryOperator::BitwiseShiftLeft
            }
            .into())
        );
        assert_eq!(
            U8(1).bitwise_shift_left(&I64(100)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: U8(1),
                rhs: U32(100),
                operator: NumericBinaryOperator::BitwiseShiftLeft
            }
            .into())
        );
        assert_eq!(
            U16(1).bitwise_shift_left(&I64(100)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: U16(1),
                rhs: U32(100),
                operator: NumericBinaryOperator::BitwiseShiftLeft
            }
            .into())
        );
        assert_eq!(
            U32(1).bitwise_shift_left(&I64(100)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: U32(1),
                rhs: U32(100),
                operator: NumericBinaryOperator::BitwiseShiftLeft
            }
            .into())
        );
        assert_eq!(
            U64(1).bitwise_shift_left(&I64(100)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: U64(1),
                rhs: U32(100),
                operator: NumericBinaryOperator::BitwiseShiftLeft
            }
            .into())
        );
        assert_eq!(
            U128(1).bitwise_shift_left(&I64(150)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: U128(1),
                rhs: U32(150),
                operator: NumericBinaryOperator::BitwiseShiftLeft
            }
            .into())
        );

        // cast error test
        assert_eq!(
            I64(1).bitwise_shift_left(&I64(-2)),
            Err(ConvertError {
                value: I64(-2),
                data_type: DataType::Uint32,
            }
            .into())
        );

        // non numeric test
        assert_eq!(
            mon!(3).bitwise_shift_left(&I64(2)),
            Err(ValueError::NonNumericMathOperation {
                lhs: mon!(3),
                rhs: U32(2),
                operator: NumericBinaryOperator::BitwiseShiftLeft,
            }
            .into())
        );

        // null test
        macro_rules! null_test {
            ($op: ident $a: expr, $b: expr) => {
                assert!($a.$op(&$b).unwrap().is_null());
            };
        }

        null_test!(bitwise_shift_left   I64(1), Null);
        null_test!(bitwise_shift_left   Null, I64(1));
    }

    #[test]
    fn bitwise_shift_right() {
        use {super::convert::ConvertError, crate::ast::DataType};

        macro_rules! test {
            ($op: ident $a: expr, $b: expr => $c: expr) => {
                assert!($a.$op(&$b).unwrap().evaluate_eq(&$c));
            };
        }

        macro_rules! mon {
            ($n: expr) => {
                Interval(Interval::Month($n))
            };
        }

        // operation result test
        test!(bitwise_shift_right I8(1),     I64(2) => I8(0));
        test!(bitwise_shift_right I16(1),    I64(2) => I16(0));
        test!(bitwise_shift_right I32(1),    I64(2) => I32(0));
        test!(bitwise_shift_right I64(1),    I64(2) => I64(0));
        test!(bitwise_shift_right I128(1),   I64(2) => I128(0));
        test!(bitwise_shift_right U8(1),     I64(2) => U8(0));
        test!(bitwise_shift_right U16(1),    I64(2) => U16(0));
        test!(bitwise_shift_right U32(1),    I64(2) => U32(0));
        test!(bitwise_shift_right U64(1),    I64(2) => U64(0));
        test!(bitwise_shift_right U128(1),   I64(2) => U128(0));
        test!(bitwise_shift_right I8(1),     U32(2) => I8(0));
        test!(bitwise_shift_right I16(1),    U32(2) => I16(0));
        test!(bitwise_shift_right I32(1),    U32(2) => I32(0));
        test!(bitwise_shift_right I64(1),    U32(2) => I64(0));
        test!(bitwise_shift_right I128(1),   U32(2) => I128(0));
        test!(bitwise_shift_right U8(1),     U32(2) => U8(0));
        test!(bitwise_shift_right U16(1),    U32(2) => U16(0));
        test!(bitwise_shift_right U32(1),    U32(2) => U32(0));
        test!(bitwise_shift_right U64(1),    U32(2) => U64(0));
        test!(bitwise_shift_right U128(1),   U32(2) => U128(0));

        //overflow test
        assert_eq!(
            I8(1).bitwise_shift_right(&I64(100)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: I8(1),
                rhs: U32(100),
                operator: NumericBinaryOperator::BitwiseShiftRight
            }
            .into())
        );
        assert_eq!(
            I16(1).bitwise_shift_right(&I64(100)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: I16(1),
                rhs: U32(100),
                operator: NumericBinaryOperator::BitwiseShiftRight
            }
            .into())
        );
        assert_eq!(
            I32(1).bitwise_shift_right(&I64(100)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: I32(1),
                rhs: U32(100),
                operator: NumericBinaryOperator::BitwiseShiftRight
            }
            .into())
        );
        assert_eq!(
            I64(1).bitwise_shift_right(&I64(100)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: I64(1),
                rhs: U32(100),
                operator: NumericBinaryOperator::BitwiseShiftRight
            }
            .into())
        );
        assert_eq!(
            I128(1).bitwise_shift_right(&I64(150)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: I128(1),
                rhs: U32(150),
                operator: NumericBinaryOperator::BitwiseShiftRight
            }
            .into())
        );
        assert_eq!(
            U8(1).bitwise_shift_right(&I64(100)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: U8(1),
                rhs: U32(100),
                operator: NumericBinaryOperator::BitwiseShiftRight
            }
            .into())
        );
        assert_eq!(
            U16(1).bitwise_shift_right(&I64(100)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: U16(1),
                rhs: U32(100),
                operator: NumericBinaryOperator::BitwiseShiftRight
            }
            .into())
        );
        assert_eq!(
            U32(1).bitwise_shift_right(&I64(100)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: U32(1),
                rhs: U32(100),
                operator: NumericBinaryOperator::BitwiseShiftRight
            }
            .into())
        );
        assert_eq!(
            U64(1).bitwise_shift_right(&I64(100)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: U64(1),
                rhs: U32(100),
                operator: NumericBinaryOperator::BitwiseShiftRight
            }
            .into())
        );
        assert_eq!(
            U128(1).bitwise_shift_right(&I64(150)),
            Err(ValueError::BinaryOperationOverflow {
                lhs: U128(1),
                rhs: U32(150),
                operator: NumericBinaryOperator::BitwiseShiftRight
            }
            .into())
        );

        // cast error test
        assert_eq!(
            I64(1).bitwise_shift_right(&I64(-2)),
            Err(ConvertError {
                value: I64(-2),
                data_type: DataType::Uint32,
            }
            .into())
        );

        // non numeric test
        assert_eq!(
            mon!(3).bitwise_shift_right(&I64(2)),
            Err(ValueError::NonNumericMathOperation {
                lhs: mon!(3),
                rhs: U32(2),
                operator: NumericBinaryOperator::BitwiseShiftRight,
            }
            .into())
        );

        // null test
        macro_rules! null_test {
            ($op: ident $a: expr, $b: expr) => {
                assert!($a.$op(&$b).unwrap().is_null());
            };
        }

        null_test!(bitwise_shift_right   I64(1), Null);
        null_test!(bitwise_shift_right   Null, I64(1));
    }

    #[test]
    fn cast() {
        use {
            crate::{ast::DataType::*, data::Point, prelude::Value},
            chrono::{NaiveDate, NaiveTime},
        };

        macro_rules! cast {
            ($input: expr => $data_type: expr, $expected: expr) => {
                let found = $input.cast(&$data_type).unwrap();

                match ($expected, found) {
                    (Null, Null) => {}
                    (expected, found) => {
                        assert_eq!(expected, found);
                    }
                }
            };
        }

        let bytea = Value::Bytea(hex::decode("0abc").unwrap());
        let inet = |v| Value::Inet(IpAddr::from_str(v).unwrap());
        let point = |x, y| Value::Point(Point::new(x, y));

        // Same as
        cast!(Bool(true)            => Boolean      , Bool(true));
        cast!(Str("a".to_owned())   => Text         , Str("a".to_owned()));
        cast!(bytea                 => Bytea        , bytea);
        cast!(inet("::1")           => Inet         , inet("::1"));
        cast!(I8(1)                 => Int8         , I8(1));
        cast!(I16(1)                 => Int16         , I16(1));
        cast!(I32(1)                => Int32        , I32(1));
        cast!(I64(1)                => Int          , I64(1));
        cast!(I128(1)               => Int128       , I128(1));
        cast!(U8(1)                 => Uint8        , U8(1));
        cast!(U16(1)                 => Uint16        , U16(1));
        cast!(U32(1)                 => Uint32        , U32(1));
        cast!(U64(1)                 => Uint64        , U64(1));
        cast!(U128(1)                 => Uint128        , U128(1));
        cast!(F32(1.0_f32)              => Float32        , F32(1.0_f32));
        cast!(F64(1.0)              => Float        , F64(1.0));
        cast!(Value::Uuid(123)      => Uuid         , Value::Uuid(123));

        // Boolean
        cast!(Str("TRUE".to_owned())    => Boolean, Bool(true));
        cast!(Str("FALSE".to_owned())   => Boolean, Bool(false));
        cast!(I8(1)                     => Boolean, Bool(true));
        cast!(I8(0)                     => Boolean, Bool(false));
        cast!(I16(0)                     => Boolean, Bool(false));
        cast!(I32(1)                     => Boolean, Bool(true));
        cast!(I32(0)                     => Boolean, Bool(false));
        cast!(I64(1)                    => Boolean, Bool(true));
        cast!(I64(0)                    => Boolean, Bool(false));
        cast!(I128(1)                   => Boolean, Bool(true));
        cast!(I128(0)                   => Boolean, Bool(false));
        cast!(U8(1)                   => Boolean, Bool(true));
        cast!(U8(0)                   => Boolean, Bool(false));
        cast!(U16(1)                   => Boolean, Bool(true));
        cast!(U16(0)                   => Boolean, Bool(false));
        cast!(U32(1)                   => Boolean, Bool(true));
        cast!(U32(1)                   => Boolean, Bool(true));
        cast!(U64(1)                   => Boolean, Bool(true));
        cast!(U64(0)                   => Boolean, Bool(false));
        cast!(U128(0)                   => Boolean, Bool(false));
        cast!(U128(0)                   => Boolean, Bool(false));
        cast!(F32(1.0_f32)                  => Boolean, Bool(true));
        cast!(F32(0.0_f32)                  => Boolean, Bool(false));
        cast!(F64(1.0)                  => Boolean, Bool(true));
        cast!(F64(0.0)                  => Boolean, Bool(false));
        cast!(Null                      => Boolean, Null);

        // Integer
        cast!(Bool(true)            => Int8, I8(1));
        cast!(Bool(false)           => Int8, I8(0));
        cast!(F32(1.1_f32)              => Int8, I8(1));
        cast!(F64(1.1)              => Int8, I8(1));
        cast!(Str("11".to_owned())  => Int8, I8(11));
        cast!(Null                  => Int8, Null);

        cast!(Bool(true)            => Int32, I32(1));
        cast!(Bool(false)           => Int32, I32(0));
        cast!(F32(1.1_f32)              => Int32, I32(1));
        cast!(F64(1.1)              => Int32, I32(1));
        cast!(Str("11".to_owned())  => Int32, I32(11));
        cast!(Null                  => Int32, Null);

        cast!(Bool(true)            => Int, I64(1));
        cast!(Bool(false)           => Int, I64(0));
        cast!(F32(1.1_f32)              => Int, I64(1));
        cast!(F64(1.1)              => Int, I64(1));
        cast!(Str("11".to_owned())  => Int, I64(11));
        cast!(Null                  => Int, Null);

        cast!(Bool(true)            => Int128, I128(1));
        cast!(Bool(false)           => Int128, I128(0));
        cast!(F32(1.1_f32)          => Int128, I128(1));
        cast!(F64(1.1)              => Int128, I128(1));
        cast!(Str("11".to_owned())  => Int128, I128(11));
        cast!(Null                  => Int128, Null);

        cast!(Bool(true)            => Uint8, U8(1));
        cast!(Bool(false)           => Uint8, U8(0));
        cast!(F32(1.1_f32)              => Uint8, U8(1));
        cast!(F64(1.1)              => Uint8, U8(1));
        cast!(Str("11".to_owned())  => Uint8, U8(11));
        cast!(Null                  => Uint8, Null);

        cast!(Bool(true)            => Uint16, U16(1));
        cast!(Bool(false)           => Uint16, U16(0));
        cast!(F32(1.1_f32)              => Uint16, U16(1));
        cast!(F64(1.1)              => Uint16, U16(1));
        cast!(Str("11".to_owned())  => Uint16, U16(11));
        cast!(Null                  => Uint16, Null);

        cast!(Bool(true)            => Uint32, U32(1));
        cast!(Bool(false)           => Uint32, U32(0));
        cast!(F32(1.1_f32)              => Uint32, U32(1));
        cast!(F64(1.1)              => Uint32, U32(1));
        cast!(Str("11".to_owned())  => Uint32, U32(11));
        cast!(Null                  => Uint32, Null);

        cast!(Bool(true)            => Uint64, U64(1));
        cast!(Bool(false)           => Uint64, U64(0));
        cast!(F32(1.1_f32)              => Uint64, U64(1));
        cast!(F64(1.1)              => Uint64, U64(1));
        cast!(Str("11".to_owned())  => Uint64, U64(11));
        cast!(Null                  => Uint64, Null);

        cast!(Bool(true)            => Uint128, U128(1));
        cast!(Bool(false)           => Uint128, U128(0));
        cast!(F32(1.1_f32)              => Uint128, U128(1));
        cast!(F64(1.1)              => Uint128, U128(1));
        cast!(Str("11".to_owned())  => Uint128, U128(11));
        cast!(Null                  => Uint128, Null);

        // Float32
        cast!(Bool(true)            => Float32, F32(1.0_f32));
        cast!(Bool(false)           => Float32, F32(0.0_f32));
        cast!(I8(1)                 => Float32, F32(1.0_f32));
        cast!(I16(1)                 => Float32, F32(1.0_f32));
        cast!(I32(1)                => Float32, F32(1.0_f32));
        cast!(I64(1)                => Float32, F32(1.0_f32));
        cast!(I128(1)               => Float32, F32(1.0_f32));
        cast!(F64(1.0)               => Float32, F32(1.0_f32));

        // Float
        cast!(Bool(true)            => Float, F64(1.0));
        cast!(Bool(false)           => Float, F64(0.0));
        cast!(I8(1)                 => Float, F64(1.0));
        cast!(I16(1)                 => Float, F64(1.0));
        cast!(I32(1)                => Float, F64(1.0));
        cast!(I64(1)                => Float, F64(1.0));
        cast!(I128(1)               => Float, F64(1.0));
        cast!(F32(1_f32)               => Float, F64(1.0));

        cast!(U8(1)                 => Float, F64(1.0));
        cast!(U16(1)                 => Float, F64(1.0));
        cast!(U32(1)                 => Float, F64(1.0));
        cast!(U64(1)                 => Float, F64(1.0));
        cast!(U128(1)                 => Float, F64(1.0));
        cast!(Str("11".to_owned())  => Float, F64(11.0));
        cast!(Null                  => Float, Null);

        // Text
        cast!(Bool(true)    => Text, Str("TRUE".to_owned()));
        cast!(Bool(false)   => Text, Str("FALSE".to_owned()));
        cast!(I8(11)        => Text, Str("11".to_owned()));
        cast!(I16(11)        => Text, Str("11".to_owned()));
        cast!(I32(11)        => Text, Str("11".to_owned()));
        cast!(I64(11)       => Text, Str("11".to_owned()));
        cast!(I128(11)        => Text, Str("11".to_owned()));
        cast!(U8(11)        => Text, Str("11".to_owned()));
        cast!(U16(11)        => Text, Str("11".to_owned()));
        cast!(U32(11)        => Text, Str("11".to_owned()));
        cast!(U64(11)        => Text, Str("11".to_owned()));
        cast!(U128(11)        => Text, Str("11".to_owned()));
        cast!(F32(1.0_f32)      => Text, Str("1".to_owned()));
        cast!(F64(1.0)      => Text, Str("1".to_owned()));
        cast!(inet("::1")    => Text, Str("::1".to_owned()));

        let date = Value::Date(NaiveDate::from_ymd_opt(2021, 5, 1).unwrap());
        cast!(date          => Text, Str("2021-05-01".to_owned()));

        let timestamp = Value::Timestamp(
            NaiveDate::from_ymd_opt(2021, 5, 1)
                .unwrap()
                .and_hms_opt(12, 34, 50)
                .unwrap(),
        );
        cast!(timestamp     => Text, Str("2021-05-01 12:34:50".to_owned()));
        cast!(Null          => Text, Null);

        // Date
        let date = Value::Date(NaiveDate::from_ymd_opt(2021, 5, 1).unwrap());
        let timestamp = Value::Timestamp(
            NaiveDate::from_ymd_opt(2021, 5, 1)
                .unwrap()
                .and_hms_opt(12, 34, 50)
                .unwrap(),
        );

        cast!(Str("2021-05-01".to_owned()) => Date, date.to_owned());
        cast!(timestamp                    => Date, date);
        cast!(Null                         => Date, Null);

        // Time
        cast!(Str("08:05:30".to_owned()) => Time, Value::Time(NaiveTime::from_hms_opt(8, 5, 30).unwrap()));
        cast!(Null                       => Time, Null);

        // Timestamp
        cast!(Value::Date(NaiveDate::from_ymd_opt(2021, 5, 1).unwrap()) => Timestamp, Value::Timestamp(NaiveDate::from_ymd_opt(2021, 5, 1).unwrap().and_hms_opt(0, 0, 0).unwrap()));
        cast!(Str("2021-05-01 08:05:30".to_owned())                     => Timestamp, Value::Timestamp(NaiveDate::from_ymd_opt(2021, 5, 1).unwrap().and_hms_opt(8, 5, 30).unwrap()));
        cast!(Null                                                      => Timestamp, Null);

        // Bytea
        cast!(Value::Str("0abc".to_owned()) => Bytea, Value::Bytea(hex::decode("0abc").unwrap()));
        assert_eq!(
            Value::Str("!@#$5".to_owned()).cast(&Bytea),
            Err(ValueError::CastFromHexToByteaFailed("!@#$5".to_owned()).into()),
        );

        // Inet
        cast!(inet("::1") => Inet, inet("::1"));
        cast!(Str("::1".to_owned()) => Inet, inet("::1"));
        cast!(Str("0.0.0.0".to_owned()) => Inet, inet("0.0.0.0"));

        // Point
        cast!(point(0.32, 0.52) => Point, point(0.32, 0.52));
        cast!(Str("POINT(0.32 0.52)".to_owned()) => Point, point(0.32, 0.52));

        // Map
        cast!(
            Str(r#"{"a": 1}"#.to_owned()) => Map,
            Value::parse_json_map(r#"{"a": 1}"#).unwrap()
        );

        // List
        cast!(
            Str(r#"[1, 2, 3]"#.to_owned()) => List,
            Value::parse_json_list(r#"[1, 2, 3]"#).unwrap()
        );

        // Casting error
        assert_eq!(
            Value::Uuid(123).cast(&List),
            Err(ValueError::UnimplementedCast {
                value: Value::Uuid(123),
                data_type: List,
            }
            .into())
        );
    }

    #[test]
    fn concat() {
        assert_eq!(
            Str("A".to_owned()).concat(Str("B".to_owned())),
            Str("AB".to_owned())
        );
        assert_eq!(
            Str("A".to_owned()).concat(Bool(true)),
            Str("ATRUE".to_owned())
        );
        assert_eq!(Str("A".to_owned()).concat(I8(1)), Str("A1".to_owned()));
        assert_eq!(Str("A".to_owned()).concat(I16(1)), Str("A1".to_owned()));
        assert_eq!(Str("A".to_owned()).concat(I32(1)), Str("A1".to_owned()));
        assert_eq!(Str("A".to_owned()).concat(I64(1)), Str("A1".to_owned()));
        assert_eq!(Str("A".to_owned()).concat(I128(1)), Str("A1".to_owned()));
        assert_eq!(Str("A".to_owned()).concat(U8(1)), Str("A1".to_owned()));
        assert_eq!(Str("A".to_owned()).concat(U16(1)), Str("A1".to_owned()));
        assert_eq!(Str("A".to_owned()).concat(U32(1)), Str("A1".to_owned()));
        assert_eq!(Str("A".to_owned()).concat(U64(1)), Str("A1".to_owned()));
        assert_eq!(Str("A".to_owned()).concat(U128(1)), Str("A1".to_owned()));
        assert_eq!(
            Str("A".to_owned()).concat(F32(1.0_f32)),
            Str("A1".to_owned())
        );
        assert_eq!(Str("A".to_owned()).concat(F64(1.0)), Str("A1".to_owned()));
        assert_eq!(
            List(vec![I64(1)]).concat(List(vec![I64(2)])),
            List(vec![I64(1), I64(2)])
        );
        assert_eq!(I64(2).concat(I64(1)), Str("21".to_owned()));
        assert!(Str("A".to_owned()).concat(Null).is_null());
    }

    #[test]
    fn validate_type() {
        use {
            super::{Value, ValueError},
            crate::{ast::DataType as D, data::Interval as I, data::Point},
            chrono::{NaiveDate, NaiveTime},
        };

        let date = Date(NaiveDate::from_ymd_opt(2021, 5, 1).unwrap());
        let timestamp = Timestamp(
            NaiveDate::from_ymd_opt(2021, 5, 1)
                .unwrap()
                .and_hms_opt(12, 34, 50)
                .unwrap(),
        );
        let time = Time(NaiveTime::from_hms_opt(12, 30, 11).unwrap());
        let interval = Interval(I::hours(5));
        let uuid = Uuid(parse_uuid("936DA01F9ABD4d9d80C702AF85C822A8").unwrap());
        let point = Point(Point::new(1.0, 2.0));
        let map = Value::parse_json_map(r#"{ "a": 10 }"#).unwrap();
        let list = Value::parse_json_list(r#"[ true ]"#).unwrap();
        let bytea = Bytea(hex::decode("9001").unwrap());
        let inet = Inet(IpAddr::from_str("::1").unwrap());

        assert!(Bool(true).validate_type(&D::Boolean).is_ok());
        assert!(Bool(true).validate_type(&D::Int).is_err());
        assert!(I8(1).validate_type(&D::Int8).is_ok());
        assert!(I8(1).validate_type(&D::Text).is_err());
        assert!(I16(1).validate_type(&D::Text).is_err());
        assert!(I32(1).validate_type(&D::Int32).is_ok());
        assert!(I32(1).validate_type(&D::Text).is_err());
        assert!(I64(1).validate_type(&D::Int).is_ok());
        assert!(I64(1).validate_type(&D::Text).is_err());
        assert!(I128(1).validate_type(&D::Int128).is_ok());
        assert!(I128(1).validate_type(&D::Text).is_err());
        assert!(U8(1).validate_type(&D::Uint8).is_ok());
        assert!(U8(1).validate_type(&D::Text).is_err());
        assert!(U16(1).validate_type(&D::Uint16).is_ok());
        assert!(U16(1).validate_type(&D::Text).is_err());
        assert!(U32(1).validate_type(&D::Uint32).is_ok());
        assert!(U32(1).validate_type(&D::Text).is_err());
        assert!(U64(1).validate_type(&D::Uint64).is_ok());
        assert!(U64(1).validate_type(&D::Text).is_err());
        assert!(U128(1).validate_type(&D::Uint128).is_ok());
        assert!(U128(1).validate_type(&D::Text).is_err());
        assert!(F32(1.0_f32).validate_type(&D::Float32).is_ok());
        assert!(F32(1.0_f32).validate_type(&D::Int).is_err());
        assert!(F64(1.0).validate_type(&D::Float).is_ok());
        assert!(F64(1.0).validate_type(&D::Int).is_err());
        assert!(Decimal(rust_decimal::Decimal::ONE)
            .validate_type(&D::Decimal)
            .is_ok());
        assert!(Decimal(rust_decimal::Decimal::ONE)
            .validate_type(&D::Int)
            .is_err());
        assert!(Str("a".to_owned()).validate_type(&D::Text).is_ok());
        assert!(Str("a".to_owned()).validate_type(&D::Int).is_err());
        assert!(bytea.validate_type(&D::Bytea).is_ok());
        assert!(bytea.validate_type(&D::Uuid).is_err());
        assert!(inet.validate_type(&D::Inet).is_ok());
        assert!(inet.validate_type(&D::Uuid).is_err());
        assert!(inet.validate_type(&D::Inet).is_ok());
        assert!(inet.validate_type(&D::Uuid).is_err());
        assert!(date.validate_type(&D::Date).is_ok());
        assert!(date.validate_type(&D::Text).is_err());
        assert!(timestamp.validate_type(&D::Timestamp).is_ok());
        assert!(timestamp.validate_type(&D::Boolean).is_err());
        assert!(time.validate_type(&D::Time).is_ok());
        assert!(time.validate_type(&D::Date).is_err());
        assert!(interval.validate_type(&D::Interval).is_ok());
        assert!(interval.validate_type(&D::Date).is_err());
        assert!(uuid.validate_type(&D::Uuid).is_ok());
        assert!(uuid.validate_type(&D::Boolean).is_err());
        assert!(point.validate_type(&D::Point).is_ok());
        assert!(point.validate_type(&D::Boolean).is_err());
        assert!(map.validate_type(&D::Map).is_ok());
        assert!(map.validate_type(&D::Int).is_err());
        assert!(list.validate_type(&D::List).is_ok());
        assert!(list.validate_type(&D::Int).is_err());
        assert!(Null.validate_type(&D::Time).is_ok());
        assert!(Null.validate_type(&D::Boolean).is_ok());

        assert_eq!(
            Bool(true).validate_type(&D::Text),
            Err(ValueError::IncompatibleDataType {
                data_type: D::Text,
                value: Bool(true),
            }
            .into()),
        );
    }

    #[test]
    fn unary_minus() {
        use crate::data::Interval as I;
        assert_eq!(I8(1).unary_minus(), Ok(I8(-1)));
        assert_eq!(I16(1).unary_minus(), Ok(I16(-1)));
        assert_eq!(I32(1).unary_minus(), Ok(I32(-1)));
        assert_eq!(I64(1).unary_minus(), Ok(I64(-1)));
        assert_eq!(I128(1).unary_minus(), Ok(I128(-1)));

        assert_eq!(F32(1.0_f32).unary_minus(), Ok(F32(-1.0)));
        assert_eq!(F64(1.0).unary_minus(), Ok(F64(-1.0)));
        assert_eq!(
            Interval(I::hours(5)).unary_minus(),
            Ok(Interval(I::hours(-5)))
        );
        assert_eq!(Null.unary_minus(), Ok(Null));
        assert_eq!(
            Decimal(Decimal::ONE).unary_minus(),
            Ok(Decimal(-Decimal::ONE))
        );

        assert_eq!(
            Str("abc".to_owned()).unary_minus(),
            Err(ValueError::UnaryMinusOnNonNumeric.into())
        );
    }

    #[test]
    fn unary_plus() {
        assert_eq!(U8(1).unary_plus(), Ok(U8(1)));
        assert!(Null.unary_plus().unwrap().is_null());
    }

    #[test]
    fn factorial() {
        assert_eq!(I8(5).unary_factorial(), Ok(I128(120)));
        assert_eq!(I16(5).unary_factorial(), Ok(I128(120)));
        assert_eq!(I32(5).unary_factorial(), Ok(I128(120)));
        assert_eq!(I64(5).unary_factorial(), Ok(I128(120)));
        assert_eq!(I128(5).unary_factorial(), Ok(I128(120)));
        assert_eq!(U8(5).unary_factorial(), Ok(I128(120)));
        assert_eq!(U16(5).unary_factorial(), Ok(I128(120)));
        assert_eq!(U32(5).unary_factorial(), Ok(I128(120)));
        assert_eq!(U64(5).unary_factorial(), Ok(I128(120)));
        assert_eq!(U128(5).unary_factorial(), Ok(I128(120)));
        assert_eq!(
            F32(5.0_f32).unary_factorial(),
            Err(ValueError::FactorialOnNonInteger.into())
        );
        assert_eq!(
            F64(5.0).unary_factorial(),
            Err(ValueError::FactorialOnNonInteger.into())
        );
        assert!(Null.unary_factorial().unwrap().is_null());
        assert_eq!(
            Str("5".to_owned()).unary_factorial(),
            Err(ValueError::FactorialOnNonNumeric.into())
        );
    }

    #[test]
    fn sqrt() {
        assert_eq!(I8(9).sqrt(), Ok(F64(3.0)));
        assert_eq!(I16(9).sqrt(), Ok(F64(3.0)));
        assert_eq!(I64(9).sqrt(), Ok(F64(3.0)));
        assert_eq!(I128(9).sqrt(), Ok(F64(3.0)));
        assert_eq!(U8(9).sqrt(), Ok(F64(3.0)));
        assert_eq!(U16(9).sqrt(), Ok(F64(3.0)));
        assert_eq!(U32(9).sqrt(), Ok(F64(3.0)));
        assert_eq!(U64(9).sqrt(), Ok(F64(3.0)));
        assert_eq!(U128(9).sqrt(), Ok(F64(3.0)));
        assert_eq!(F32(9.0_f32).sqrt(), Ok(F64(3.0)));
        assert_eq!(F64(9.0).sqrt(), Ok(F64(3.0)));
        assert!(Null.sqrt().unwrap().is_null());
        assert_eq!(
            Str("9".to_owned()).sqrt(),
            Err(ValueError::SqrtOnNonNumeric(Str("9".to_owned())).into())
        );
    }

    #[test]
    fn bitwise_and() {
        macro_rules! test {
            ($op: ident $a: expr, $b: expr => $c: expr) => {
                assert!($a.$op(&$b).unwrap().evaluate_eq(&$c));
            };
        }

        macro_rules! test_bitwise_and {
            ($($vt: ident $pt: ident);*;) => {
                $(
                    test!(bitwise_and $vt($pt::MIN), $vt($pt::MIN) => $vt($pt::MIN & $pt::MIN));
                    test!(bitwise_and $vt($pt::MIN), $vt($pt::MAX) => $vt($pt::MIN & $pt::MAX));
                    test!(bitwise_and $vt($pt::MAX), $vt($pt::MAX) => $vt($pt::MAX & $pt::MAX));
                    test!(bitwise_and $vt(0), $vt(0) => $vt(0 & 0));
                    test!(bitwise_and $vt(0), $vt(1) => $vt(0 & 1));
                    test!(bitwise_and $vt(1), $vt(0) => $vt(1 & 0));
                    test!(bitwise_and $vt(1), $vt(1) => $vt(1 & 1));
                )*
            };
        }

        test_bitwise_and!(
            I8      i8;
            I16     i16;
            I32     i32;
            I64     i64;
            I128    i128;
            U8      u8;
            U16     u16;
            U32     u32;
            U64     u64;
            U128    u128;
        );

        macro_rules! null_test {
            ($op: ident $a: expr, $b: expr) => {
                assert!($a.$op(&$b).unwrap().is_null());
            };
        }

        macro_rules! null_test_bitwise_and {
            ($($vt: ident)*) => {
                $(
                    null_test!(bitwise_and $vt(1), Null);
                    null_test!(bitwise_and Null, $vt(1));
                    null_test!(bitwise_and Null, Null);
                )*
            };
        }

        null_test_bitwise_and!(
            I8 I16 I32 I64 I128 U8 U16 U32 U64 U128
        );

        let lhs = I8(3);
        let rhs = I16(12);
        assert_eq!(
            lhs.bitwise_and(&rhs),
            Err(ValueError::NonNumericMathOperation {
                lhs,
                rhs,
                operator: NumericBinaryOperator::BitwiseAnd
            }
            .into())
        )
    }

    #[test]
    fn position() {
        let str1 = Str("ramen".to_owned());
        let str2 = Str("men".to_owned());
        let empty_str = Str("".to_owned());

        assert_eq!(str1.position(&str2), Ok(I64(3)));
        assert_eq!(str2.position(&str1), Ok(I64(0)));
        assert!(Null.position(&str2).unwrap().is_null());
        assert!(str1.position(&Null).unwrap().is_null());
        assert_eq!(empty_str.position(&str2), Ok(I64(0)));
        assert_eq!(str1.position(&empty_str), Ok(I64(0)));
        assert_eq!(
            str1.position(&I64(1)),
            Err(ValueError::NonStringParameterInPosition {
                from: str1,
                sub: I64(1)
            }
            .into())
        );
    }

    #[test]
    fn get_type() {
        use {
            super::Value,
            crate::{ast::DataType as D, data::Interval as I, data::Point},
            chrono::{NaiveDate, NaiveTime},
        };

        let decimal = Decimal(rust_decimal::Decimal::ONE);
        let date = Date(NaiveDate::from_ymd_opt(2021, 5, 1).unwrap());
        let timestamp = Timestamp(
            NaiveDate::from_ymd_opt(2021, 5, 1)
                .unwrap()
                .and_hms_opt(12, 34, 50)
                .unwrap(),
        );
        let time = Time(NaiveTime::from_hms_opt(12, 30, 11).unwrap());
        let interval = Interval(I::hours(5));
        let uuid = Uuid(parse_uuid("936DA01F9ABD4d9d80C702AF85C822A8").unwrap());
        let point = Point(Point::new(1.0, 2.0));
        let map = Value::parse_json_map(r#"{ "a": 10 }"#).unwrap();
        let list = Value::parse_json_list(r#"[ true ]"#).unwrap();
        let bytea = Bytea(hex::decode("9001").unwrap());
        let inet = Inet(IpAddr::from_str("::1").unwrap());

        assert_eq!(I8(1).get_type(), Some(D::Int8));
        assert_eq!(I16(1).get_type(), Some(D::Int16));
        assert_eq!(I32(1).get_type(), Some(D::Int32));
        assert_eq!(I64(1).get_type(), Some(D::Int));
        assert_eq!(I128(1).get_type(), Some(D::Int128));
        assert_eq!(U8(1).get_type(), Some(D::Uint8));
        assert_eq!(U16(1).get_type(), Some(D::Uint16));
        assert_eq!(U32(1).get_type(), Some(D::Uint32));
        assert_eq!(U64(1).get_type(), Some(D::Uint64));
        assert_eq!(U128(1).get_type(), Some(D::Uint128));
        assert_eq!(F32(1.1_f32).get_type(), Some(D::Float32));
        assert_eq!(F64(1.1).get_type(), Some(D::Float));
        assert_eq!(decimal.get_type(), Some(D::Decimal));
        assert_eq!(Bool(true).get_type(), Some(D::Boolean));
        assert_eq!(Str('1'.into()).get_type(), Some(D::Text));
        assert_eq!(bytea.get_type(), Some(D::Bytea));
        assert_eq!(inet.get_type(), Some(D::Inet));
        assert_eq!(date.get_type(), Some(D::Date));
        assert_eq!(timestamp.get_type(), Some(D::Timestamp));
        assert_eq!(time.get_type(), Some(D::Time));
        assert_eq!(interval.get_type(), Some(D::Interval));
        assert_eq!(uuid.get_type(), Some(D::Uuid));
        assert_eq!(point.get_type(), Some(D::Point));
        assert_eq!(map.get_type(), Some(D::Map));
        assert_eq!(list.get_type(), Some(D::List));
        assert_eq!(Null.get_type(), None);
    }
}
