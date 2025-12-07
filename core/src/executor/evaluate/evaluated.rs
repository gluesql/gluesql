use {
    self::convert::{cast_number_to_value, cast_text_to_value, number_to_value, text_to_value},
    super::{error::EvaluateError, function},
    crate::{
        ast::{DataType, TrimWhereField},
        data::{BigDecimalExt, Key, Value, ValueError, value::BTreeMapJsonExt},
        result::{Error, Result},
    },
    bigdecimal::BigDecimal,
    std::{
        borrow::Cow,
        collections::BTreeMap,
        convert::TryFrom,
        fmt::{Display, Formatter},
        ops::Range,
    },
    utils::Tribool,
    uuid::Uuid,
};

mod binary_op;
mod cmp;
mod concat;
pub(super) mod convert;
mod eq;
mod like;
mod unary_op;

#[derive(Clone, Debug, PartialEq)]
pub enum Evaluated<'a> {
    Number(Cow<'a, BigDecimal>),
    Text(Cow<'a, str>),
    StrSlice {
        source: Cow<'a, str>,
        range: Range<usize>,
    },
    Value(Value),
}

/// Formats the evaluated value as a string.
/// This is primarily intended for error message generation, not for general-purpose display.
impl Display for Evaluated<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Evaluated::Number(v) => write!(f, "{v}"),
            Evaluated::Text(v) => write!(f, "{v}"),
            Evaluated::StrSlice { source, range } => write!(f, "{}", &source[range.clone()]),
            Evaluated::Value(v) => write!(f, "{}", String::from(v)),
        }
    }
}

impl TryFrom<Evaluated<'_>> for Value {
    type Error = Error;

    fn try_from(e: Evaluated<'_>) -> Result<Value> {
        match e {
            Evaluated::Number(value) => {
                let decimal = value.as_ref();

                decimal
                    .to_i64()
                    .map(Value::I64)
                    .or_else(|| decimal.to_f64().filter(|f| f.is_finite()).map(Value::F64))
                    .ok_or_else(|| {
                        EvaluateError::NumberParseFailed {
                            literal: decimal.to_string(),
                            data_type: DataType::Float,
                        }
                        .into()
                    })
            }
            Evaluated::Text(value) => Ok(Value::Str(value.into_owned())),
            Evaluated::StrSlice {
                source: s,
                range: r,
            } => Ok(Value::Str(s[r].to_owned())),
            Evaluated::Value(v) => Ok(v),
        }
    }
}

impl TryFrom<Evaluated<'_>> for Key {
    type Error = Error;

    fn try_from(evaluated: Evaluated<'_>) -> Result<Self> {
        Self::try_from(&evaluated)
    }
}

impl TryFrom<&Evaluated<'_>> for Key {
    type Error = Error;

    fn try_from(evaluated: &Evaluated<'_>) -> Result<Self> {
        match evaluated {
            Evaluated::Number(_) | Evaluated::Text(_) => {
                Value::try_from(evaluated.clone())?.try_into()
            }
            Evaluated::StrSlice { source, range } => Ok(Key::Str(source[range.clone()].to_owned())),
            Evaluated::Value(v) => v.try_into(),
        }
    }
}

impl TryFrom<Evaluated<'_>> for bool {
    type Error = Error;

    fn try_from(evaluated: Evaluated<'_>) -> Result<bool> {
        let v = match evaluated {
            Evaluated::Value(Value::Bool(v)) => return Ok(v),
            Evaluated::Number(value) => value.to_string(),
            Evaluated::Text(value) => value.to_string(),
            Evaluated::StrSlice { source, range } => source[range].to_owned(),
            Evaluated::Value(value) => String::from(value),
        };

        Err(EvaluateError::BooleanTypeRequired(v).into())
    }
}

impl TryFrom<Evaluated<'_>> for BTreeMap<String, Value> {
    type Error = Error;

    fn try_from(evaluated: Evaluated<'_>) -> Result<BTreeMap<String, Value>> {
        match evaluated {
            Evaluated::Text(v) => BTreeMap::parse_json_object(v.as_ref()),
            Evaluated::Number(v) => Err(EvaluateError::TextLiteralRequired(v.to_string()).into()),
            Evaluated::Value(Value::Str(v)) => BTreeMap::parse_json_object(v.as_str()),
            Evaluated::Value(Value::Map(v)) => Ok(v),
            Evaluated::Value(v) => Err(EvaluateError::MapOrStringValueRequired(v.into()).into()),
            Evaluated::StrSlice { source, range } => BTreeMap::parse_json_object(&source[range]),
        }
    }
}

pub fn exceptional_int_val_to_eval<'a>(name: String, v: &Value) -> Result<Evaluated<'a>> {
    match v {
        Value::Null => Ok(Evaluated::Value(Value::Null)),
        _ => Err(EvaluateError::FunctionRequiresIntegerValue(name).into()),
    }
}

impl From<Tribool> for Evaluated<'_> {
    fn from(x: Tribool) -> Self {
        Evaluated::Value(Value::from(x))
    }
}

impl<'a> Evaluated<'a> {
    pub fn arrow<'b>(&'a self, other: &Evaluated<'b>) -> Result<Evaluated<'b>> {
        let selector = Value::try_from(other.clone())?;

        if selector.is_null() {
            return Ok(Evaluated::Value(Value::Null));
        }

        let value_result = if let Evaluated::Value(base) = self {
            function::select_arrow_value(base, &selector)
        } else {
            let base = Value::try_from(self.clone())?;
            function::select_arrow_value(&base, &selector)
        };

        value_result.map(Evaluated::Value)
    }

    pub fn long_arrow<'b>(&'a self, other: &Evaluated<'b>) -> Result<Evaluated<'b>> {
        let selector = Value::try_from(other.clone())?;

        if selector.is_null() {
            return Ok(Evaluated::Value(Value::Null));
        }

        let value_result = if let Evaluated::Value(base) = self {
            function::select_long_arrow_value(base, &selector)
        } else {
            let base = Value::try_from(self.clone())?;
            function::select_long_arrow_value(&base, &selector)
        };

        value_result.map(Evaluated::Value)
    }

    pub fn cast(self, data_type: &DataType) -> Result<Evaluated<'a>> {
        match self {
            Evaluated::Number(value) => cast_number_to_value(data_type, value.as_ref()),
            Evaluated::Text(value) => cast_text_to_value(data_type, value.as_ref()),
            Evaluated::Value(value) => value.cast(data_type),
            Evaluated::StrSlice { source, range } => {
                Value::Str(source[range].to_owned()).cast(data_type)
            }
        }
        .map(Evaluated::Value)
    }

    pub fn ltrim(self, name: String, chars: Option<Evaluated<'_>>) -> Result<Evaluated<'a>> {
        let (source, range) = match self {
            Evaluated::Text(l) => {
                let end = l.len();
                (l, 0..end)
            }
            Evaluated::Value(Value::Null) => {
                return Ok(Evaluated::Value(Value::Null));
            }
            Evaluated::StrSlice { source, range } => (source, range),
            Evaluated::Value(Value::Str(v)) => {
                let end = v.len();
                (Cow::Owned(v), 0..end)
            }
            _ => return Err(EvaluateError::FunctionRequiresStringValue(name).into()),
        };

        let filter_chars = match chars {
            Some(expr) => match expr.try_into()? {
                Value::Str(value) => value,
                Value::Null => {
                    return Ok(Evaluated::Value(Value::Null));
                }
                _ => {
                    return Err(EvaluateError::FunctionRequiresStringValue(name).into());
                }
            }
            .chars()
            .collect::<Vec<_>>(),
            None => vec![' '],
        };
        let sliced_expr = &source[range.clone()];
        let matched_vec: Vec<_> = sliced_expr.match_indices(&filter_chars[..]).collect();

        //"x".trim_start_matches(['x','y','z']) => ""
        if matched_vec.len() == sliced_expr.len() {
            return Ok(Evaluated::StrSlice {
                source,
                range: 0..0,
            });
        }
        //"tuv".trim_start_matches(['x','y','z']) => "tuv"
        if matched_vec.is_empty() {
            return Ok(Evaluated::StrSlice { source, range });
        }
        //"txu".trim_start_matches(['x','y','z']) => "txu"
        if matched_vec[0].0 != 0 && matched_vec[matched_vec.len() - 1].0 != sliced_expr.len() - 1 {
            return Ok(Evaluated::StrSlice { source, range });
        }
        let pivot = matched_vec
            .iter()
            .enumerate()
            .skip_while(|(vec_idx, (slice_idx, _))| vec_idx == slice_idx)
            .map(|(vec_idx, (_, _))| vec_idx)
            .next();

        let start = match pivot {
            Some(idx) => match idx {
                0 => 0,
                _ => matched_vec[idx - 1].0 + 1,
            },
            _ => matched_vec[matched_vec.len() - 1].0 + 1,
        };

        Ok(Evaluated::StrSlice {
            source,
            range: range.start + start..range.end,
        })
    }

    pub fn is_null(&self) -> bool {
        matches!(self, Evaluated::Value(v) if v.is_null())
    }

    pub fn rtrim(self, name: String, chars: Option<Evaluated<'_>>) -> Result<Evaluated<'a>> {
        let (source, range) = match self {
            Evaluated::Text(l) => {
                let end = l.len();
                (l, 0..end)
            }
            Evaluated::Value(Value::Null) => {
                return Ok(Evaluated::Value(Value::Null));
            }
            Evaluated::StrSlice { source, range } => (source, range),
            Evaluated::Value(Value::Str(v)) => {
                let end = v.len();
                (Cow::Owned(v), 0..end)
            }
            _ => return Err(EvaluateError::FunctionRequiresStringValue(name).into()),
        };

        let filter_chars = match chars {
            Some(expr) => match expr.try_into()? {
                Value::Str(value) => value,
                Value::Null => {
                    return Ok(Evaluated::Value(Value::Null));
                }
                _ => {
                    return Err(EvaluateError::FunctionRequiresStringValue(name).into());
                }
            }
            .chars()
            .collect::<Vec<_>>(),
            None => vec![' '],
        };
        let sliced_expr = &source[range.clone()];
        let matched_vec: Vec<_> = sliced_expr.match_indices(&filter_chars[..]).collect();

        //"x".trim_end_matches(['x','y','z']) => ""
        if matched_vec.len() == sliced_expr.len() {
            return Ok(Evaluated::StrSlice {
                source,
                range: 0..0,
            });
        }
        //"tuv".trim_end_matches(['x','y','z']) => "tuv"
        if matched_vec.is_empty() {
            return Ok(Evaluated::StrSlice { source, range });
        }
        //"txu".trim_end_matches(['x','y','z']) => "txu"
        if matched_vec[0].0 != 0 && matched_vec[matched_vec.len() - 1].0 != sliced_expr.len() - 1 {
            return Ok(Evaluated::StrSlice { source, range });
        }

        let pivot = matched_vec
            .iter()
            .rev()
            .enumerate()
            .skip_while(|(vec_idx, (slice_idx, _))| *vec_idx == sliced_expr.len() - slice_idx - 1)
            .map(|(vec_idx, (_, _))| vec_idx)
            .next();

        let end = match pivot {
            Some(idx) => match idx {
                0 => range.end,
                _ => matched_vec[matched_vec.len() - idx].0,
            },
            _ => matched_vec[0].0,
        };

        Ok(Evaluated::StrSlice {
            source,
            range: range.start..end,
        })
    }

    pub fn substr(
        self,
        name: String,
        start: Evaluated<'a>,
        count: Option<Evaluated<'a>>,
    ) -> Result<Evaluated<'a>> {
        let (source, range) = match self {
            Evaluated::Text(l) => {
                let end = l.len();
                (l, 0..end)
            }
            Evaluated::Value(Value::Null) => {
                return Ok(Evaluated::Value(Value::Null));
            }
            Evaluated::StrSlice { source, range } => (source, range),
            Evaluated::Value(Value::Str(v)) => {
                let end = v.len();
                (Cow::Owned(v), 0..end)
            }
            _ => return Err(EvaluateError::FunctionRequiresStringValue(name).into()),
        };

        let start = {
            let value = start.try_into()?;
            match value {
                Value::I64(num) => num,
                _ => return exceptional_int_val_to_eval(name, &value),
            }
        } - 1;

        let count = match count {
            Some(eval) => {
                let value = eval.try_into()?;
                match value {
                    Value::I64(num) => num,
                    _ => return exceptional_int_val_to_eval(name, &value),
                }
            }
            None => source.len() as i64,
        };

        let end = if count < 0 {
            return Err(EvaluateError::NegativeSubstrLenNotAllowed.into());
        } else {
            (range.start as i64 + start + count).clamp(0, source.len() as i64) as usize
        };

        let start = (start + range.start as i64).clamp(0, source.len() as i64) as usize;

        Ok(Evaluated::StrSlice {
            source,
            range: start..end,
        })
    }

    pub fn trim(
        self,
        name: String,
        filter_chars: Option<Evaluated<'_>>,
        trim_where_field: Option<&TrimWhereField>,
    ) -> Result<Evaluated<'a>> {
        let (source, range) = match self {
            Evaluated::Text(l) => {
                let end = l.len();
                (l, 0..end)
            }
            Evaluated::Value(Value::Null) => {
                return Ok(Evaluated::Value(Value::Null));
            }
            Evaluated::StrSlice { source, range } => (source, range),
            Evaluated::Value(Value::Str(v)) => {
                let end = v.len();
                (Cow::Owned(v), 0..end)
            }
            _ => return Err(EvaluateError::FunctionRequiresStringValue(name).into()),
        };

        let filter_chars = match filter_chars {
            Some(expr) => match expr.try_into()? {
                Value::Str(value) => value,
                Value::Null => {
                    return Ok(Evaluated::Value(Value::Null));
                }
                _ => {
                    return Err(EvaluateError::FunctionRequiresStringValue(name).into());
                }
            }
            .chars()
            .collect::<Vec<_>>(),
            None => vec![' '],
        };
        let sliced_expr = &source[range.clone()];
        let matched_vec: Vec<_> = sliced_expr.match_indices(&filter_chars[..]).collect();
        //filter_chars => ['x','y','z']
        //"x".trim_matches(filter_chars[..]) => ""
        if matched_vec.len() == sliced_expr.len() {
            return Ok(Evaluated::StrSlice {
                source,
                range: 0..0,
            });
        }
        //filter_chars => ['x','y','z']
        //"tuv".trim_matches(filter_chars[..]) => "tuv"
        if matched_vec.is_empty() {
            return Ok(Evaluated::StrSlice { source, range });
        }
        //filter_chars => ['x','y','z']
        //"txu".trim_matches(filter_chars[..]) => "txu"
        if matched_vec[0].0 != 0 && matched_vec[matched_vec.len() - 1].0 != sliced_expr.len() - 1 {
            return Ok(Evaluated::StrSlice { source, range });
        }
        match trim_where_field {
            Some(TrimWhereField::Both) => {
                //filter_chars => ['x','y','z']
                //"xyzbyxlxyz  ".trim_matches(filter_chars[..]) => "byxlxyz  "
                if matched_vec[0].0 == 0
                    && matched_vec[matched_vec.len() - 1].0 != sliced_expr.len() - 1
                {
                    let pivot = matched_vec
                        .iter()
                        .enumerate()
                        .skip_while(|(vec_idx, (slice_idx, _))| vec_idx == slice_idx)
                        .map(|(vec_idx, (_, _))| vec_idx)
                        .next();

                    let start = match pivot {
                        Some(idx) => matched_vec[idx - 1].0 + 1,
                        _ => matched_vec[matched_vec.len() - 1].0 + 1,
                    };

                    return Ok(Evaluated::StrSlice {
                        source,
                        range: range.start + start..range.end,
                    });
                }
                //filter_chars => ['x','y','z']
                //"  xyzblankxyzxx".trim_matches(filter_chars[..]) => "  xyzblank"
                if matched_vec[0].0 != 0
                    && matched_vec[matched_vec.len() - 1].0 == sliced_expr.len() - 1
                {
                    let pivot = matched_vec
                        .iter()
                        .rev()
                        .enumerate()
                        .skip_while(|(vec_idx, (slice_idx, _))| {
                            *vec_idx == sliced_expr.len() - slice_idx - 1
                        })
                        .map(|(vec_idx, (_, _))| vec_idx)
                        .next();

                    let end = match pivot {
                        Some(idx) => matched_vec[matched_vec.len() - idx].0,
                        _ => matched_vec[0].0,
                    };

                    return Ok(Evaluated::StrSlice {
                        source,
                        range: range.start..end,
                    });
                }
                //filter_chars => ['x','y','z']
                //"xxbyz".trim_matches(filter_chars[..]) => "b"
                let pivot = matched_vec
                    .iter()
                    .enumerate()
                    .skip_while(|(vec_idx, (slice_idx, _))| vec_idx == slice_idx)
                    .map(|(vec_idx, (_, _))| vec_idx)
                    .next()
                    .unwrap_or(0);

                let trim_range = matched_vec[pivot - 1].0..(matched_vec[pivot].0 + range.start);

                Ok(Evaluated::StrSlice {
                    source,
                    range: range.start + trim_range.start + 1..trim_range.end,
                })
            }
            Some(TrimWhereField::Leading) => {
                let pivot = matched_vec
                    .iter()
                    .enumerate()
                    .skip_while(|(vec_idx, (slice_idx, _))| vec_idx == slice_idx)
                    .map(|(vec_idx, (_, _))| vec_idx)
                    .next();

                let start = match pivot {
                    Some(idx) => match idx {
                        0 => 0,
                        _ => matched_vec[idx - 1].0 + 1,
                    },
                    _ => matched_vec[matched_vec.len() - 1].0 + 1,
                };

                Ok(Evaluated::StrSlice {
                    source,
                    range: range.start + start..range.end,
                })
            }
            Some(TrimWhereField::Trailing) => {
                let pivot = matched_vec
                    .iter()
                    .rev()
                    .enumerate()
                    .skip_while(|(vec_idx, (slice_idx, _))| {
                        *vec_idx == sliced_expr.len() - slice_idx - 1
                    })
                    .map(|(vec_idx, (_, _))| vec_idx)
                    .next();

                let end = match pivot {
                    Some(idx) => match idx {
                        0 => range.end,
                        _ => matched_vec[matched_vec.len() - idx].0,
                    },
                    _ => matched_vec[0].0,
                };

                Ok(Evaluated::StrSlice {
                    source,
                    range: range.start..end,
                })
            }
            None => {
                let start = source
                    .chars()
                    .skip(range.start)
                    .enumerate()
                    .find(|(_, c)| !c.is_whitespace())
                    .map_or(0, |(idx, _)| idx + range.start);

                let end = source.len()
                    - source
                        .chars()
                        .rev()
                        .skip(source.len() - range.end)
                        .enumerate()
                        .find(|(_, c)| !c.is_whitespace())
                        .map_or(0, |(idx, _)| source.len() - (range.end - idx));

                Ok(Evaluated::StrSlice {
                    source,
                    range: start..end,
                })
            }
        }
    }

    pub fn try_into_value(self, data_type: &DataType, nullable: bool) -> Result<Value> {
        let value = match self {
            Evaluated::Number(value) => number_to_value(data_type, value.as_ref())?,
            Evaluated::Text(value) => text_to_value(data_type, value.as_ref())?,
            Evaluated::Value(Value::Bytea(bytes)) if data_type == &DataType::Uuid => {
                Uuid::from_slice(&bytes)
                    .map_err(|_| ValueError::FailedToParseUUID(hex::encode(bytes)))
                    .map(|uuid| Value::Uuid(uuid.as_u128()))?
            }
            Evaluated::Value(value) => value,
            Evaluated::StrSlice {
                source: s,
                range: r,
            } => Value::Str(s[r].to_owned()),
        };

        value.validate_type(data_type)?;
        value.validate_null(nullable)?;

        Ok(value)
    }
}

#[cfg(test)]
mod tests {
    use {
        super::Evaluated,
        crate::{ast::DataType, data::Value, executor::EvaluateError},
        bigdecimal::BigDecimal,
        std::{borrow::Cow, collections::BTreeMap, str::FromStr},
    };

    #[test]
    fn try_from_evaluated_to_value() {
        let num = |s: &str| Evaluated::Number(Cow::Owned(BigDecimal::from_str(s).unwrap()));
        let test = |e, result| assert_eq!(Value::try_from(e), result);

        test(num("42"), Ok(Value::I64(42)));
        test(num("2.5"), Ok(Value::F64(2.5)));
        test(
            num("1e400"),
            Err(EvaluateError::NumberParseFailed {
                literal: "1e+400".to_owned(),
                data_type: DataType::Float,
            }
            .into()),
        );
        test(
            Evaluated::Text(Cow::Owned("hello".to_owned())),
            Ok(Value::Str("hello".to_owned())),
        );
        test(
            Evaluated::StrSlice {
                source: Cow::Owned("hello world".to_owned()),
                range: 0..5,
            },
            Ok(Value::Str("hello".to_owned())),
        );
        test(Evaluated::Value(Value::Bool(true)), Ok(Value::Bool(true)));
    }

    #[test]
    fn display() {
        let num = |s: &str| Evaluated::Number(Cow::Owned(BigDecimal::from_str(s).unwrap()));
        let text = |s: &str| Evaluated::Text(Cow::Owned(s.to_owned()));
        let slice = |s: &'static str| Evaluated::StrSlice {
            source: Cow::Borrowed(s),
            range: 0..s.len(),
        };
        let val = |v| Evaluated::Value(v);

        assert_eq!(num("42").to_string(), "42");
        assert_eq!(num("3.14").to_string(), "3.14");
        assert_eq!(text("hello").to_string(), "hello");
        assert_eq!(slice("world").to_string(), "world");
        assert_eq!(val(Value::Bool(true)).to_string(), "TRUE");
        assert_eq!(val(Value::Null).to_string(), "NULL");
        assert_eq!(val(Value::Str("foo".to_owned())).to_string(), "foo");
    }

    #[test]
    fn try_from_evaluated_to_btreemap() {
        let expected = || Ok([("a".to_owned(), Value::I64(1))].into_iter().collect());
        let test = |e, result| assert_eq!(BTreeMap::try_from(e), result);

        test(
            Evaluated::Text(Cow::Owned(r#"{"a": 1}"#.to_owned())),
            expected(),
        );
        test(
            Evaluated::Number(Cow::Owned(BigDecimal::from_str("42").unwrap())),
            Err(EvaluateError::TextLiteralRequired("42".to_owned()).into()),
        );
        test(
            Evaluated::Value(Value::Str(r#"{"a": 1}"#.to_owned())),
            expected(),
        );
        test(
            Evaluated::Value(Value::Map(expected().unwrap())),
            expected(),
        );
        test(
            Evaluated::Value(Value::Bool(true)),
            Err(EvaluateError::MapOrStringValueRequired("TRUE".to_owned()).into()),
        );
        test(
            Evaluated::StrSlice {
                source: Cow::Owned(r#"{"a": 1}"#.to_owned()),
                range: 0..8,
            },
            expected(),
        );
    }
}
