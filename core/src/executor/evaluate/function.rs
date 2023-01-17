use {
    super::{EvaluateError, Evaluated},
    crate::{
        ast::{DataType, DateTimeField},
        data::{Value, ValueError},
        result::Result,
    },
    rand::{rngs::StdRng, Rng, SeedableRng},
    std::{
        cmp::{max, min},
        ops::ControlFlow,
    },
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

// --- float ---

pub fn abs<'a>(name: String, n: Evaluated<'_>) -> Result<Evaluated<'a>> {
    match n.try_into()? {
        Value::I8(v) => Ok(Evaluated::from(Value::I8(v.abs()))),
        Value::I64(v) => Ok(Evaluated::from(Value::I64(v.abs()))),
        Value::Decimal(v) => Ok(Evaluated::from(Value::Decimal(v.abs()))),
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

    Ok(Evaluated::from(Value::I64(gcd_i64(left, right))))
}

pub fn lcm<'a>(name: String, left: Evaluated<'_>, right: Evaluated<'_>) -> Result<Evaluated<'a>> {
    let left = eval_to_int!(name, left);
    let right = eval_to_int!(name, right);

    fn lcm(a: i64, b: i64) -> i64 {
        a * b / gcd_i64(a, b)
    }

    Ok(Evaluated::from(Value::I64(lcm(left, right))))
}

fn gcd_i64(a: i64, b: i64) -> i64 {
    if b == 0 {
        a
    } else {
        gcd_i64(b, a % b)
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

pub fn cast<'a>(expr: Evaluated<'a>, data_type: &DataType) -> Result<Evaluated<'a>> {
    expr.cast(data_type)
}

pub fn extract<'a>(field: &DateTimeField, expr: Evaluated<'_>) -> Result<Evaluated<'a>> {
    Ok(Evaluated::from(Value::try_from(expr)?.extract(field)?))
}
