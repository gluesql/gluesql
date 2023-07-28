use chrono::Datelike;

use {
    super::{EvaluateError, Evaluated},
    crate::{
        ast::{DataType, DateTimeField},
        data::{Key, Point, Value, ValueError},
        result::{Error, Result},
    },
    chrono::{Datelike, Duration, Months},
    md5::{Digest, Md5},
    rand::{rngs::StdRng, Rng, SeedableRng},
    std::ops::ControlFlow,
    uuid::Uuid,
};

macro_rules! eval_to_str {
    ($name: expr, $evaluated: expr) => {
        match $evaluated.try_into()? {
            Value::Str(value) => value,
            Value::Null => {
                return Ok(Evaluated::from(Value::Null));
            }
            _ => {
                return Err(EvaluateError::FunctionRequiresStringValue($name).into());
            }
        }
    };
}

macro_rules! eval_to_int {
    ($name: expr, $evaluated: expr) => {
        match $evaluated.try_into()? {
            Value::I64(num) => num,
            Value::Null => {
                return Ok(Evaluated::from(Value::Null));
            }
            _ => {
                return Err(EvaluateError::FunctionRequiresIntegerValue($name).into());
            }
        }
    };
}

macro_rules! eval_to_float {
    ($name: expr, $evaluated: expr) => {
        match $evaluated.try_into()? {
            Value::I64(v) => v as f64,
            Value::F32(v) => v.into(),
            Value::F64(v) => v,
            Value::Null => {
                return Ok(Evaluated::from(Value::Null));
            }
            _ => {
                return Err(EvaluateError::FunctionRequiresFloatValue($name).into());
            }
        }
    };
}

macro_rules! eval_to_point {
    ($name: expr, $evaluated: expr) => {
        match $evaluated.try_into()? {
            Value::Point(v) => v,
            Value::Null => {
                return Ok(Evaluated::from(Value::Null));
            }
            _ => {
                return Err(EvaluateError::FunctionRequiresPointValue($name).into());
            }
        }
    };
}

// --- text ---

pub fn concat(exprs: Vec<Evaluated<'_>>) -> Result<Evaluated> {
    enum BreakCase {
        Null,
        Err(crate::result::Error),
    }

    let control_flow = exprs.into_iter().map(|expr| expr.try_into()).try_fold(
        None,
        |left: Option<Value>, right: Result<Value>| match (left, right) {
            (_, Ok(value)) if value.is_null() => ControlFlow::Break(BreakCase::Null),
            (_, Err(err)) => ControlFlow::Break(BreakCase::Err(err)),
            (Some(left), Ok(value)) => ControlFlow::Continue(Some(left.concat(value))),
            (None, Ok(value)) => ControlFlow::Continue(Some(value)),
        },
    );

    match control_flow {
        ControlFlow::Continue(Some(value)) => Ok(Evaluated::from(value)),
        ControlFlow::Continue(None) => Err(ValueError::EmptyArgNotAllowedInConcat.into()),
        ControlFlow::Break(BreakCase::Null) => Ok(Evaluated::from(Value::Null)),
        ControlFlow::Break(BreakCase::Err(err)) => Err(err),
    }
}

pub fn concat_ws<'a>(
    name: String,
    separator: Evaluated<'a>,
    exprs: Vec<Evaluated<'a>>,
) -> Result<Evaluated<'a>> {
    let separator = eval_to_str!(name, separator);

    let result = exprs
        .into_iter()
        .map(Value::try_from)
        .filter(|value| !matches!(value, Ok(Value::Null)))
        .map(|value| Ok(String::from(value?)))
        .collect::<Result<Vec<_>>>()?
        .join(&separator);

    Ok(Evaluated::from(Value::Str(result)))
}

pub fn lower(name: String, expr: Evaluated<'_>) -> Result<Evaluated> {
    Ok(Evaluated::from(Value::Str(
        eval_to_str!(name, expr).to_lowercase(),
    )))
}

pub fn initcap(name: String, expr: Evaluated<'_>) -> Result<Evaluated> {
    let string = eval_to_str!(name, expr);
    let string = string
        .chars()
        .scan(true, |state, c| {
            let c = if *state {
                c.to_ascii_uppercase()
            } else {
                c.to_ascii_lowercase()
            };
            *state = !c.is_alphanumeric();
            Some(c)
        })
        .collect();
    Ok(Evaluated::from(Value::Str(string)))
}

pub fn upper(name: String, expr: Evaluated<'_>) -> Result<Evaluated> {
    Ok(Evaluated::from(Value::Str(
        eval_to_str!(name, expr).to_uppercase(),
    )))
}

pub fn left_or_right<'a>(
    name: String,
    expr: Evaluated<'_>,
    size: Evaluated<'_>,
) -> Result<Evaluated<'a>> {
    let string = eval_to_str!(name, expr);
    let size = match size.try_into()? {
        Value::I64(number) => usize::try_from(number)
            .map_err(|_| EvaluateError::FunctionRequiresUSizeValue(name.clone()))?,
        Value::Null => {
            return Ok(Evaluated::Value(Value::Null));
        }
        _ => {
            return Err(EvaluateError::FunctionRequiresIntegerValue(name).into());
        }
    };

    let converted = if name == "LEFT" {
        string.get(..size).map(|v| v.to_owned()).unwrap_or(string)
    } else {
        let start_pos = if size > string.len() {
            0
        } else {
            string.len() - size
        };

        string
            .get(start_pos..)
            .map(|value| value.to_owned())
            .unwrap_or(string)
    };

    Ok(Evaluated::from(Value::Str(converted)))
}

pub fn lpad_or_rpad<'a>(
    name: String,
    expr: Evaluated<'_>,
    size: Evaluated<'_>,
    fill: Option<Evaluated<'_>>,
) -> Result<Evaluated<'a>> {
    let string = eval_to_str!(name, expr);
    let size = match size.try_into()? {
        Value::I64(number) => usize::try_from(number)
            .map_err(|_| EvaluateError::FunctionRequiresUSizeValue(name.clone()))?,
        Value::Null => {
            return Ok(Evaluated::Value(Value::Null));
        }
        _ => {
            return Err(EvaluateError::FunctionRequiresIntegerValue(name).into());
        }
    };

    let fill = match fill {
        Some(expr) => eval_to_str!(name, expr),
        None => " ".to_owned(),
    };

    let result = if size > string.len() {
        let padding_size = size - string.len();
        let repeat_count = padding_size / fill.len();
        let plus_count = padding_size % fill.len();
        let fill = fill.repeat(repeat_count) + &fill[0..plus_count];

        if name == "LPAD" {
            fill + &string
        } else {
            string + &fill
        }
    } else {
        string[0..size].to_owned()
    };

    Ok(Evaluated::from(Value::Str(result)))
}

pub fn reverse(name: String, expr: Evaluated<'_>) -> Result<Evaluated> {
    let value = eval_to_str!(name, expr).chars().rev().collect::<String>();

    Ok(Evaluated::from(Value::Str(value)))
}

pub fn repeat<'a>(name: String, expr: Evaluated<'_>, num: Evaluated<'_>) -> Result<Evaluated<'a>> {
    let expr = eval_to_str!(name, expr);
    let num = eval_to_int!(name, num) as usize;
    let value = expr.repeat(num);

    Ok(Evaluated::from(Value::Str(value)))
}

pub fn replace<'a>(
    name: String,
    expr: Evaluated<'_>,
    old: Evaluated<'_>,
    new: Evaluated<'_>,
) -> Result<Evaluated<'a>> {
    let expr = eval_to_str!(name, expr);
    let old = eval_to_str!(name, old);
    let new = eval_to_str!(name, new);
    let value = expr.replace(&old, &new);
    Ok(Evaluated::from(Value::Str(value)))
}

pub fn ascii<'a>(name: String, expr: Evaluated<'_>) -> Result<Evaluated<'a>> {
    let string = eval_to_str!(name, expr);
    let mut iter = string.chars();

    match (iter.next(), iter.next()) {
        (Some(c), None) => {
            if c.is_ascii() {
                Ok(Evaluated::from(Value::U8(c as u8)))
            } else {
                Err(EvaluateError::NonAsciiCharacterNotAllowed.into())
            }
        }
        _ => Err(EvaluateError::AsciiFunctionRequiresSingleCharacterValue.into()),
    }
}

pub fn chr<'a>(name: String, expr: Evaluated<'_>) -> Result<Evaluated<'a>> {
    let expr = eval_to_int!(name, expr);

    match expr {
        0..=255 => {
            let expr = expr as u8;
            Ok(Evaluated::from(Value::Str((expr as char).to_string())))
        }
        _ => Err(EvaluateError::ChrFunctionRequiresIntegerValueInRange0To255.into()),
    }
}

pub fn md5<'a>(name: String, expr: Evaluated<'_>) -> Result<Evaluated<'a>> {
    let string = eval_to_str!(name, expr);
    let mut hasher = Md5::new();
    hasher.update(string.as_bytes());
    let result = hasher.finalize();
    let result = format!("{:x}", result);

    Ok(Evaluated::from(Value::Str(result)))
}

// --- float ---

pub fn abs<'a>(name: String, n: Evaluated<'_>) -> Result<Evaluated<'a>> {
    match n.try_into()? {
        Value::I8(v) => Ok(Evaluated::from(Value::I8(v.abs()))),
        Value::I32(v) => Ok(Evaluated::from(Value::I32(v.abs()))),
        Value::I64(v) => Ok(Evaluated::from(Value::I64(v.abs()))),
        Value::I128(v) => Ok(Evaluated::from(Value::I128(v.abs()))),
        Value::Decimal(v) => Ok(Evaluated::from(Value::Decimal(v.abs()))),
        Value::F32(v) => Ok(Evaluated::from(Value::F32(v.abs()))),
        Value::F64(v) => Ok(Evaluated::from(Value::F64(v.abs()))),
        Value::Null => Ok(Evaluated::from(Value::Null)),
        _ => Err(EvaluateError::FunctionRequiresFloatValue(name).into()),
    }
}

pub fn ifnull<'a>(expr: Evaluated<'a>, then: Evaluated<'a>) -> Result<Evaluated<'a>> {
    Ok(match expr.is_null() {
        true => then,
        false => expr,
    })
}

pub fn sign(name: String, n: Evaluated<'_>) -> Result<Evaluated> {
    let x = eval_to_float!(name, n);
    if x == 0.0 {
        return Ok(Evaluated::from(Value::I8(0)));
    }
    Ok(Evaluated::from(Value::I8(x.signum() as i8)))
}

pub fn sqrt<'a>(n: Evaluated<'_>) -> Result<Evaluated<'a>> {
    Ok(Evaluated::from(Value::sqrt(&n.try_into()?)?))
}

pub fn power<'a>(name: String, expr: Evaluated<'_>, power: Evaluated<'_>) -> Result<Evaluated<'a>> {
    let expr = eval_to_float!(name, expr);
    let power = eval_to_float!(name, power);

    Ok(Evaluated::from(Value::F64(expr.powf(power))))
}

pub fn ceil<'a>(name: String, n: Evaluated<'_>) -> Result<Evaluated<'a>> {
    Ok(Evaluated::from(Value::F64(eval_to_float!(name, n).ceil())))
}

pub fn rand<'a>(name: String, seed: Option<Evaluated<'_>>) -> Result<Evaluated<'a>> {
    let seed = if let Some(v) = seed {
        StdRng::seed_from_u64(eval_to_float!(name, v) as u64).gen()
    } else {
        rand::random()
    };
    Ok(Evaluated::from(Value::F64(seed)))
}

pub fn round<'a>(name: String, n: Evaluated<'_>) -> Result<Evaluated<'a>> {
    Ok(Evaluated::from(Value::F64(eval_to_float!(name, n).round())))
}

pub fn floor<'a>(name: String, n: Evaluated<'_>) -> Result<Evaluated<'a>> {
    Ok(Evaluated::from(Value::F64(eval_to_float!(name, n).floor())))
}

pub fn radians<'a>(name: String, n: Evaluated<'_>) -> Result<Evaluated<'a>> {
    Ok(Evaluated::from(Value::F64(
        eval_to_float!(name, n).to_radians(),
    )))
}

pub fn degrees<'a>(name: String, n: Evaluated<'_>) -> Result<Evaluated<'a>> {
    Ok(Evaluated::from(Value::F64(
        eval_to_float!(name, n).to_degrees(),
    )))
}

pub fn exp<'a>(name: String, n: Evaluated<'_>) -> Result<Evaluated<'a>> {
    Ok(Evaluated::from(Value::F64(eval_to_float!(name, n).exp())))
}

pub fn log<'a>(name: String, antilog: Evaluated<'_>, base: Evaluated<'_>) -> Result<Evaluated<'a>> {
    let antilog = eval_to_float!(name, antilog);
    let base = eval_to_float!(name, base);

    Ok(Evaluated::from(Value::F64(antilog.log(base))))
}

pub fn ln<'a>(name: String, n: Evaluated<'_>) -> Result<Evaluated<'a>> {
    Ok(Evaluated::from(Value::F64(eval_to_float!(name, n).ln())))
}

pub fn log2<'a>(name: String, n: Evaluated<'_>) -> Result<Evaluated<'a>> {
    Ok(Evaluated::from(Value::F64(eval_to_float!(name, n).log2())))
}

pub fn log10<'a>(name: String, n: Evaluated<'_>) -> Result<Evaluated<'a>> {
    Ok(Evaluated::from(Value::F64(eval_to_float!(name, n).log10())))
}

pub fn sin<'a>(name: String, n: Evaluated<'_>) -> Result<Evaluated<'a>> {
    Ok(Evaluated::from(Value::F64(eval_to_float!(name, n).sin())))
}

pub fn cos<'a>(name: String, n: Evaluated<'_>) -> Result<Evaluated<'a>> {
    Ok(Evaluated::from(Value::F64(eval_to_float!(name, n).cos())))
}

pub fn tan<'a>(name: String, n: Evaluated<'_>) -> Result<Evaluated<'a>> {
    Ok(Evaluated::from(Value::F64(eval_to_float!(name, n).tan())))
}

pub fn asin<'a>(name: String, n: Evaluated<'_>) -> Result<Evaluated<'a>> {
    Ok(Evaluated::from(Value::F64(eval_to_float!(name, n).asin())))
}

pub fn acos<'a>(name: String, n: Evaluated<'_>) -> Result<Evaluated<'a>> {
    Ok(Evaluated::from(Value::F64(eval_to_float!(name, n).acos())))
}

pub fn atan<'a>(name: String, n: Evaluated<'_>) -> Result<Evaluated<'a>> {
    Ok(Evaluated::from(Value::F64(eval_to_float!(name, n).atan())))
}

// --- integer ---

pub fn div<'a>(
    name: String,
    dividend: Evaluated<'_>,
    divisor: Evaluated<'_>,
) -> Result<Evaluated<'a>> {
    let dividend = match dividend.try_into()? {
        Value::F32(number) => number as f64,
        Value::F64(number) => number,
        Value::I64(number) => number as f64,
        Value::Null => {
            return Ok(Evaluated::from(Value::Null));
        }
        _ => {
            return Err(EvaluateError::FunctionRequiresFloatOrIntegerValue(name).into());
        }
    };

    let divisor = match divisor.try_into()? {
        Value::F32(number) => match number {
            x if x == 0.0 => return Err(EvaluateError::DivisorShouldNotBeZero.into()),
            _ => number as f64,
        },
        Value::F64(number) => match number {
            x if x == 0.0 => return Err(EvaluateError::DivisorShouldNotBeZero.into()),
            _ => number,
        },
        Value::I64(number) => match number {
            0 => return Err(EvaluateError::DivisorShouldNotBeZero.into()),
            _ => number as f64,
        },
        Value::Null => {
            return Ok(Evaluated::from(Value::Null));
        }
        _ => {
            return Err(EvaluateError::FunctionRequiresFloatOrIntegerValue(name).into());
        }
    };

    Ok(Evaluated::from(Value::I64((dividend / divisor) as i64)))
}

pub fn gcd<'a>(name: String, left: Evaluated<'_>, right: Evaluated<'_>) -> Result<Evaluated<'a>> {
    let left = eval_to_int!(name, left);
    let right = eval_to_int!(name, right);

    Ok(Evaluated::from(Value::I64(gcd_i64(left, right)?)))
}

pub fn lcm<'a>(name: String, left: Evaluated<'_>, right: Evaluated<'_>) -> Result<Evaluated<'a>> {
    let left = eval_to_int!(name, left);
    let right = eval_to_int!(name, right);

    fn lcm(a: i64, b: i64) -> Result<i64> {
        let gcd_val: i128 = gcd_i64(a, b)?.into();

        let a: i128 = a.into();
        let b: i128 = b.into();

        // lcm(a, b) = abs(a * b) / gcd(a, b)   if gcd(a, b) != 0
        // lcm(a, b) = 0                        if gcd(a, b) == 0
        let result = (a * b).abs().checked_div(gcd_val).unwrap_or(0);

        i64::try_from(result).map_err(|_| Error::Value(ValueError::LcmResultOutOfRange))
    }

    Ok(Evaluated::from(Value::I64(lcm(left, right)?)))
}

fn gcd_i64(a: i64, b: i64) -> Result<i64> {
    let mut a = a
        .checked_abs()
        .ok_or(Error::Value(ValueError::GcdLcmOverflow(a)))?;
    let mut b = b
        .checked_abs()
        .ok_or(Error::Value(ValueError::GcdLcmOverflow(b)))?;

    while b > 0 {
        (a, b) = (b, a % b);
    }

    Ok(a)
}

// --- list ---
pub fn append<'a>(expr: Evaluated<'_>, value: Evaluated<'_>) -> Result<Evaluated<'a>> {
    let expr: Value = expr.try_into()?;
    let value: Value = value.try_into()?;

    match (expr, value) {
        (Value::List(mut l), v) => {
            l.push(v);
            Ok(Evaluated::Value(Value::List(l)))
        }
        _ => Err(EvaluateError::ListTypeRequired.into()),
    }
}

pub fn prepend<'a>(expr: Evaluated<'_>, value: Evaluated<'_>) -> Result<Evaluated<'a>> {
    let expr: Value = expr.try_into()?;
    let value: Value = value.try_into()?;

    match (expr, value) {
        (Value::List(mut l), v) => {
            l.insert(0, v);
            Ok(Evaluated::Value(Value::List(l)))
        }
        _ => Err(EvaluateError::ListTypeRequired.into()),
    }
}

pub fn skip<'a>(name: String, expr: Evaluated<'_>, size: Evaluated<'_>) -> Result<Evaluated<'a>> {
    if expr.is_null() || size.is_null() {
        return Ok(Evaluated::Value(Value::Null));
    }
    let expr: Value = expr.try_into()?;
    let size: usize = match size.try_into()? {
        Value::I64(number) => {
            usize::try_from(number).map_err(|_| EvaluateError::FunctionRequiresUSizeValue(name))?
        }
        _ => {
            return Err(EvaluateError::FunctionRequiresIntegerValue(name).into());
        }
    };

    match expr {
        Value::List(l) => {
            let l = l.into_iter().skip(size).collect();
            Ok(Evaluated::Value(Value::List(l)))
        }
        _ => Err(EvaluateError::ListTypeRequired.into()),
    }
}

pub fn sort<'a>(expr: Evaluated<'_>, order: Evaluated<'_>) -> Result<Evaluated<'a>> {
    let expr: Value = expr.try_into()?;
    let order: Value = order.try_into()?;

    match expr {
        Value::List(l) => {
            let mut l: Vec<(Key, Value)> = l
                .into_iter()
                .map(|v| match Key::try_from(&v) {
                    Ok(key) => Ok((key, v)),
                    Err(_) => Err(EvaluateError::InvalidSortType),
                })
                .collect::<Result<Vec<(Key, Value)>, EvaluateError>>()?;

            let asc = match order {
                Value::Str(s) => match s.to_uppercase().as_str() {
                    "ASC" => true,
                    "DESC" => false,
                    _ => return Err(EvaluateError::InvalidSortOrder.into()),
                },
                _ => return Err(EvaluateError::InvalidSortOrder.into()),
            };

            l.sort_by(|a, b| if asc { a.0.cmp(&b.0) } else { b.0.cmp(&a.0) });

            Ok(Evaluated::Value(Value::List(
                l.into_iter().map(|(_, v)| v).collect(),
            )))
        }
        _ => Err(EvaluateError::ListTypeRequired.into()),
    }
}

pub fn slice<'a>(
    name: String,
    expr: Evaluated<'_>,
    start: Evaluated<'_>,
    length: Evaluated<'_>,
) -> Result<Evaluated<'a>> {
    let expr: Value = expr.try_into()?;
    let mut start = eval_to_int!(name, start);
    let length = match length.try_into()? {
        Value::I64(number) => {
            usize::try_from(number).map_err(|_| EvaluateError::FunctionRequiresUSizeValue(name))?
        }
        _ => {
            return Err(EvaluateError::FunctionRequiresIntegerValue(name).into());
        }
    };
    match expr {
        Value::List(l) => {
            if start < 0 {
                start += l.len() as i64;
            }
            if start < 0 {
                start = 0;
            }

            let start_usize = start as usize;

            let l = l.into_iter().skip(start_usize).take(length).collect();
            Ok(Evaluated::Value(Value::List(l)))
        }
        _ => Err(EvaluateError::ListTypeRequired.into()),
    }
}

pub fn take<'a>(name: String, expr: Evaluated<'_>, size: Evaluated<'_>) -> Result<Evaluated<'a>> {
    if expr.is_null() || size.is_null() {
        return Ok(Evaluated::Value(Value::Null));
    }

    let expr: Value = expr.try_into()?;
    let size = match size.try_into()? {
        Value::I64(number) => {
            usize::try_from(number).map_err(|_| EvaluateError::FunctionRequiresUSizeValue(name))?
        }
        _ => {
            return Err(EvaluateError::FunctionRequiresIntegerValue(name).into());
        }
    };

    match expr {
        Value::List(l) => {
            let l = l.into_iter().take(size).collect();
            Ok(Evaluated::Value(Value::List(l)))
        }
        _ => Err(EvaluateError::ListTypeRequired.into()),
    }
}

pub fn is_empty<'a>(expr: Evaluated<'_>) -> Result<Evaluated<'a>> {
    let expr: Value = expr.try_into()?;
    let length = match expr {
        Value::List(l) => l.len(),
        Value::Map(m) => m.len(),
        _ => {
            return Err(EvaluateError::MapOrListTypeRequired.into());
        }
    };

    Ok(Evaluated::from(Value::Bool(length == 0)))
}

pub fn values<'a>(expr: Evaluated<'_>) -> Result<Evaluated<'a>> {
    let expr: Value = expr.try_into()?;
    match expr {
        Value::Map(m) => Ok(Evaluated::from(Value::List(m.into_values().collect()))),
        _ => Err(EvaluateError::MapTypeRequired.into()),
    }
}

// --- etc ---

pub fn unwrap<'a>(
    name: String,
    expr: Evaluated<'_>,
    selector: Evaluated<'_>,
) -> Result<Evaluated<'a>> {
    if expr.is_null() {
        return Ok(Evaluated::from(Value::Null));
    }

    let value = match expr {
        Evaluated::Value(value) => value,
        _ => {
            return Err(EvaluateError::FunctionRequiresMapValue(name).into());
        }
    };

    let selector = eval_to_str!(name, selector);
    Ok(Evaluated::from(value.selector(&selector)?))
}

pub fn generate_uuid<'a>() -> Evaluated<'a> {
    Evaluated::from(Value::Uuid(Uuid::new_v4().as_u128()))
}

pub fn greatest(name: String, exprs: Vec<Evaluated<'_>>) -> Result<Evaluated<'_>> {
    exprs
        .into_iter()
        .try_fold(None, |greatest, expr| -> Result<_> {
            let greatest = match greatest {
                Some(greatest) => greatest,
                None => return Ok(Some(expr)),
            };

            match greatest.evaluate_cmp(&expr) {
                Some(std::cmp::Ordering::Less) => Ok(Some(expr)),
                Some(_) => Ok(Some(greatest)),
                None => Err(EvaluateError::NonComparableArgumentError(name.to_owned()).into()),
            }
        })?
        .ok_or(EvaluateError::FunctionRequiresAtLeastOneArgument(name.to_owned()).into())
}

pub fn format<'a>(
    name: String,
    expr: Evaluated<'_>,
    format: Evaluated<'_>,
) -> Result<Evaluated<'a>> {
    match expr.try_into()? {
        Value::Date(expr) => {
            let format = eval_to_str!(name, format);

            Ok(Evaluated::from(Value::Str(
                chrono::NaiveDate::format(&expr, &format).to_string(),
            )))
        }
        Value::Timestamp(expr) => {
            let format = eval_to_str!(name, format);
            Ok(Evaluated::from(Value::Str(
                chrono::NaiveDateTime::format(&expr, &format).to_string(),
            )))
        }
        Value::Time(expr) => {
            let format = eval_to_str!(name, format);
            Ok(Evaluated::from(Value::Str(
                chrono::NaiveTime::format(&expr, &format).to_string(),
            )))
        }
        value => Err(EvaluateError::UnsupportedExprForFormatFunction(value.into()).into()),
    }
}

pub fn last_day<'a>(name: String, expr: Evaluated<'_>) -> Result<Evaluated<'a>> {
    let date = match expr.try_into()? {
        Value::Date(date) => date,
        Value::Timestamp(timestamp) => timestamp.date(),
        _ => return Err(EvaluateError::FunctionRequiresDateOrDateTimeValue(name).into()),
    };

    Ok(Evaluated::from(Value::Date(
        date + Months::new(1) - Duration::days(date.day() as i64),
    )))
}

pub fn to_date<'a>(
    name: String,
    expr: Evaluated<'_>,
    format: Evaluated<'_>,
) -> Result<Evaluated<'a>> {
    match expr.try_into()? {
        Value::Str(expr) => {
            let format = eval_to_str!(name, format);

            chrono::NaiveDate::parse_from_str(&expr, &format)
                .map(Value::Date)
                .map(Evaluated::from)
                .map_err(|err| {
                    let err: EvaluateError = err.into();
                    err.into()
                })
        }
        _ => Err(EvaluateError::FunctionRequiresStringValue(name).into()),
    }
}

pub fn to_timestamp<'a>(
    name: String,
    expr: Evaluated<'_>,
    format: Evaluated<'_>,
) -> Result<Evaluated<'a>> {
    match expr.try_into()? {
        Value::Str(expr) => {
            let format = eval_to_str!(name, format);

            chrono::NaiveDateTime::parse_from_str(&expr, &format)
                .map(Value::Timestamp)
                .map(Evaluated::from)
                .map_err(|err| {
                    let err: EvaluateError = err.into();
                    err.into()
                })
        }
        _ => Err(EvaluateError::FunctionRequiresStringValue(name).into()),
    }
}

pub fn add_month(name: String, expr: Evaluated<'_>, size: Evaluated<'_>) -> Result<Evaluated<'_>> {
    match expr.try_into()? {
        Value::Str(expr) => {
            let format = eval_to_str!(name, format);
            let data = chrono::NaiveTime::parse_from_str(&expr, &format)
                .map(Value::Time)
                .map(Evaluated::from)
                .map_err(|err| {
                    let err: EvaluateError = err.into();
                    err.into()
                });
            return data;

            
            //return date;
        }
        _ => Err(EvaluateError::FunctionRequiresStringValue(name).into()),
    }
}

pub fn to_time<'a>(
    name: String,
    expr: Evaluated<'_>,
    format: Evaluated<'_>,
) -> Result<Evaluated<'a>> {
    match expr.try_into()? {
        Value::Str(expr) => {
            let format = eval_to_str!(name, format);

            chrono::NaiveTime::parse_from_str(&expr, &format)
                .map(Value::Time)
                .map(Evaluated::from)
                .map_err(|err| {
                    let err: EvaluateError = err.into();
                    err.into()
                })
        }
        _ => Err(EvaluateError::FunctionRequiresStringValue(name).into()),
    }
}

pub fn position<'a>(from_expr: Evaluated<'_>, sub_expr: Evaluated<'_>) -> Result<Evaluated<'a>> {
    let from: Value = from_expr.try_into()?;
    let sub: Value = sub_expr.try_into()?;

    from.position(&sub).map(Evaluated::from)
}

pub fn find_idx<'a>(
    name: String,
    from: Evaluated<'a>,
    sub: Evaluated<'a>,
    start: Option<Evaluated<'a>>,
) -> Result<Evaluated<'a>> {
    let from_expr = eval_to_str!(name, from);
    let sub_expr = eval_to_str!(name, sub);

    match start {
        Some(start) => {
            let start = eval_to_int!(name, start);
            Value::find_idx(
                &Value::Str(from_expr),
                &Value::Str(sub_expr),
                &Value::I64(start),
            )
        }
        None => Value::position(&Value::Str(from_expr), &Value::Str(sub_expr)),
    }
    .map(Evaluated::from)
}

pub fn cast<'a>(expr: Evaluated<'a>, data_type: &DataType) -> Result<Evaluated<'a>> {
    expr.cast(data_type)
}

pub fn extract<'a>(field: &DateTimeField, expr: Evaluated<'_>) -> Result<Evaluated<'a>> {
    Ok(Evaluated::from(Value::try_from(expr)?.extract(field)?))
}

pub fn point<'a>(x: Evaluated<'_>, y: Evaluated<'_>) -> Result<Evaluated<'a>> {
    let x = eval_to_float!("point".to_owned(), x);
    let y = eval_to_float!("point".to_owned(), y);

    Ok(Evaluated::from(Value::Point(Point::new(x, y))))
}

pub fn get_x<'a>(name: String, expr: Evaluated<'_>) -> Result<Evaluated<'a>> {
    match expr.try_into()? {
        Value::Point(v) => Ok(Evaluated::from(Value::F64(v.x))),
        _ => Err(EvaluateError::FunctionRequiresPointValue(name).into()),
    }
}

pub fn get_y<'a>(name: String, expr: Evaluated<'_>) -> Result<Evaluated<'a>> {
    match expr.try_into()? {
        Value::Point(v) => Ok(Evaluated::from(Value::F64(v.y))),
        _ => Err(EvaluateError::FunctionRequiresPointValue(name).into()),
    }
}

pub fn calc_distance<'a>(x: Evaluated<'_>, y: Evaluated<'_>) -> Result<Evaluated<'a>> {
    let x = eval_to_point!("calc_distance".to_owned(), x);
    let y = eval_to_point!("calc_distance".to_owned(), y);

    Ok(Evaluated::from(Value::F64(Point::calc_distance(&x, &y))))
}

pub fn coalesce<'a>(exprs: Vec<Evaluated<'_>>) -> Result<Evaluated<'a>> {
    if exprs.is_empty() {
        return Err((EvaluateError::FunctionRequiresMoreArguments {
            function_name: "COALESCE".to_owned(),
            required_minimum: 1,
            found: exprs.len(),
        })
        .into());
    }

    let control_flow = exprs.into_iter().map(|expr| expr.try_into()).try_for_each(
        |item: Result<Value>| match item {
            Ok(value) if value.is_null() => ControlFlow::Continue(()),
            Ok(value) => ControlFlow::Break(Ok(value)),
            Err(err) => ControlFlow::Break(Err(err)),
        },
    );

    match control_flow {
        ControlFlow::Break(Ok(value)) => Ok(Evaluated::from(value)),
        ControlFlow::Break(Err(err)) => Err(err),
        ControlFlow::Continue(()) => Ok(Evaluated::from(Value::Null)),
    }
}

pub fn length<'a>(name: String, expr: Evaluated<'_>) -> Result<Evaluated<'a>> {
    match expr.try_into()? {
        Value::Str(expr) => Ok(Evaluated::from(Value::U64(expr.chars().count() as u64))),
        Value::List(expr) => Ok(Evaluated::from(Value::U64(expr.len() as u64))),
        Value::Map(expr) => Ok(Evaluated::from(Value::U64(expr.len() as u64))),
        _ => Err(EvaluateError::FunctionRequiresStrOrListOrMapValue(name).into()),
    }
}

pub fn entries<'a>(name: String, expr: Evaluated<'_>) -> Result<Evaluated<'a>> {
    match expr.try_into()? {
        Value::Map(expr) => {
            let entries = expr
                .into_iter()
                .map(|(k, v)| Value::List(vec![Value::Str(k), v]))
                .collect::<Vec<_>>();
            Ok(Evaluated::from(Value::List(entries)))
        }
        _ => Err(EvaluateError::FunctionRequiresMapValue(name).into()),
    }
}

pub fn splice<'a>(
    name: String,
    list_data: Evaluated<'_>,
    begin_index: Evaluated<'_>,
    end_index: Evaluated<'_>,
    values: Option<Evaluated<'_>>,
) -> Result<Evaluated<'a>> {
    let list_data = match Value::try_from(list_data)? {
        Value::List(list) => Ok(list),
        _ => Err(EvaluateError::ListTypeRequired),
    }?;

    let begin_index = usize::try_from(eval_to_int!(name, begin_index).max(0))
        .map_err(|_| EvaluateError::FunctionRequiresUSizeValue(name.clone()))?;

    let end_index = usize::try_from(eval_to_int!(name, end_index).min(list_data.len() as i64))
        .map_err(|_| EvaluateError::FunctionRequiresUSizeValue(name))?;

    let (left, right) = {
        let mut list_iter = list_data.into_iter();
        let left: Vec<_> = list_iter.by_ref().take(begin_index).collect();
        let right: Vec<_> = list_iter.skip(end_index - begin_index).collect();
        (left, right)
    };

    let center = match values {
        Some(values) => match Value::try_from(values)? {
            Value::List(list) => Ok(list),
            _ => Err(EvaluateError::ListTypeRequired),
        }?,
        None => vec![],
    };

    let result = {
        let mut result = vec![];
        result.extend(left);
        result.extend(center);
        result.extend(right);
        result
    };

    Ok(Evaluated::from(Value::List(result)))
}
