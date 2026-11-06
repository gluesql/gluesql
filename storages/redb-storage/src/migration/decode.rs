//! Strict bincode decoding for the schema and row types used by these migrations.
//! Bounds nesting and collection sizes without interpreting versions or changing enum tags.

use {
    bincode::Options,
    serde::de::{
        self, DeserializeOwned, DeserializeSeed, EnumAccess, MapAccess, SeqAccess, VariantAccess,
        Visitor,
    },
    std::{cell::Cell, fmt, io::Cursor},
    thiserror::Error,
};

const MAX_DEPTH: usize = 256;

#[derive(Debug, Error)]
pub enum DecodeError {
    #[error("{0}")]
    Invalid(#[from] bincode::Error),

    #[error("migration decoding nesting limit exceeded")]
    NestingLimit,
}

pub(super) fn deserialize<T: DeserializeOwned>(payload: &[u8]) -> Result<T, DecodeError> {
    let mut reader = Cursor::new(payload);
    let nesting_limit = Cell::new(false);
    let options = bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .with_little_endian()
        .reject_trailing_bytes()
        .with_limit(payload.len() as u64);
    let mut inner = bincode::Deserializer::with_reader(&mut reader, options);
    let decoded = T::deserialize(BoundedDeserializer {
        inner: &mut inner,
        context: Context {
            depth: 0,
            max_len: payload.len(),
            nesting_limit: &nesting_limit,
        },
    });
    if nesting_limit.get() {
        return Err(DecodeError::NestingLimit);
    }
    let value = decoded?;
    // Reader-based deserialization does not check Options::reject_trailing_bytes.
    if reader.position() != payload.len() as u64 {
        return Err(DecodeError::Invalid(de::Error::custom(
            "bytes remain after the payload",
        )));
    }
    Ok(value)
}

#[derive(Clone, Copy)]
struct Context<'de> {
    depth: usize,
    max_len: usize,
    nesting_limit: &'de Cell<bool>,
}

impl Context<'_> {
    fn descend<E: de::Error>(self) -> Result<Self, E> {
        if self.depth == MAX_DEPTH {
            self.nesting_limit.set(true);
            return Err(de::Error::custom(
                "migration decoding nesting limit exceeded",
            ));
        }
        Ok(Self {
            depth: self.depth + 1,
            ..self
        })
    }
}

struct BoundedDeserializer<'de, D> {
    inner: D,
    context: Context<'de>,
}

macro_rules! forward_deserialize {
    ($($method:ident $(, $argument:ident : $argument_type:ty)*);* $(;)?) => {
        $(
            fn $method<V: Visitor<'de>>(
                self, $($argument: $argument_type,)* visitor: V,
            ) -> Result<V::Value, Self::Error> {
                let context = self.context.descend()?;
                self.inner.$method($($argument,)* WrappedVisitor {
                    inner: visitor, context,
                })
            }
        )*
    };
}

impl<'de, D: de::Deserializer<'de>> de::Deserializer<'de> for BoundedDeserializer<'de, D> {
    type Error = D::Error;

    forward_deserialize! {
        deserialize_any; deserialize_bool;
        deserialize_i8; deserialize_i16; deserialize_i32; deserialize_i64; deserialize_i128;
        deserialize_u8; deserialize_u16; deserialize_u32; deserialize_u64; deserialize_u128;
        deserialize_f32; deserialize_f64; deserialize_char; deserialize_str; deserialize_string;
        deserialize_bytes; deserialize_byte_buf; deserialize_option; deserialize_unit;
        deserialize_unit_struct, name: &'static str;
        deserialize_newtype_struct, name: &'static str;
        deserialize_seq; deserialize_tuple, len: usize;
        deserialize_tuple_struct, name: &'static str, len: usize;
        deserialize_map; deserialize_identifier; deserialize_ignored_any;
    }

    fn deserialize_struct<V: Visitor<'de>>(
        self,
        name: &'static str,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error> {
        let context = self.context.descend()?;
        self.inner.deserialize_struct(
            name,
            fields,
            WrappedVisitor {
                inner: visitor,
                context,
            },
        )
    }

    fn deserialize_enum<V: Visitor<'de>>(
        self,
        name: &'static str,
        variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error> {
        let context = self.context.descend()?;
        self.inner.deserialize_enum(
            name,
            variants,
            WrappedVisitor {
                inner: visitor,
                context,
            },
        )
    }

    fn is_human_readable(&self) -> bool {
        false
    }
}

struct WrappedSeed<'de, S> {
    inner: S,
    context: Context<'de>,
}

impl<'de, S: DeserializeSeed<'de>> DeserializeSeed<'de> for WrappedSeed<'de, S> {
    type Value = S::Value;
    fn deserialize<D: de::Deserializer<'de>>(
        self,
        deserializer: D,
    ) -> Result<Self::Value, D::Error> {
        self.inner.deserialize(BoundedDeserializer {
            inner: deserializer,
            context: self.context,
        })
    }
}

struct WrappedVisitor<'de, V> {
    inner: V,
    context: Context<'de>,
}

macro_rules! forward_visit {
    ($($method:ident, $value_type:ty);* $(;)?) => {
        $(
            fn $method<E: de::Error>(self, value: $value_type) -> Result<Self::Value, E> {
                self.inner.$method(value)
            }
        )*
    };
}

impl<'de, V: Visitor<'de>> Visitor<'de> for WrappedVisitor<'de, V> {
    type Value = V::Value;
    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.inner.expecting(formatter)
    }
    forward_visit! {
        visit_bool, bool; visit_i8, i8; visit_i16, i16; visit_i32, i32;
        visit_i64, i64; visit_i128, i128; visit_u8, u8; visit_u16, u16;
        visit_u32, u32; visit_u64, u64; visit_u128, u128; visit_f32, f32;
        visit_f64, f64; visit_str, &str; visit_string, String;
    }
    fn visit_none<E: de::Error>(self) -> Result<Self::Value, E> {
        self.inner.visit_none()
    }
    fn visit_some<D: de::Deserializer<'de>>(
        self,
        deserializer: D,
    ) -> Result<Self::Value, D::Error> {
        self.inner.visit_some(BoundedDeserializer {
            inner: deserializer,
            context: self.context,
        })
    }
    fn visit_newtype_struct<D: de::Deserializer<'de>>(
        self,
        deserializer: D,
    ) -> Result<Self::Value, D::Error> {
        self.inner.visit_newtype_struct(BoundedDeserializer {
            inner: deserializer,
            context: self.context,
        })
    }
    fn visit_seq<A: SeqAccess<'de>>(self, access: A) -> Result<Self::Value, A::Error> {
        if access
            .size_hint()
            .is_some_and(|length| length > self.context.max_len)
        {
            return Err(de::Error::custom("sequence length exceeds payload size"));
        }
        self.inner.visit_seq(WrappedAccess {
            inner: access,
            context: self.context,
        })
    }
    fn visit_map<A: MapAccess<'de>>(self, access: A) -> Result<Self::Value, A::Error> {
        if access
            .size_hint()
            .is_some_and(|length| length > self.context.max_len)
        {
            return Err(de::Error::custom("map length exceeds payload size"));
        }
        self.inner.visit_map(WrappedAccess {
            inner: access,
            context: self.context,
        })
    }
    fn visit_enum<A: EnumAccess<'de>>(self, access: A) -> Result<Self::Value, A::Error> {
        self.inner.visit_enum(WrappedEnum {
            inner: access,
            context: self.context,
        })
    }
}

struct WrappedAccess<'de, A> {
    inner: A,
    context: Context<'de>,
}

impl<'de, A: SeqAccess<'de>> SeqAccess<'de> for WrappedAccess<'de, A> {
    type Error = A::Error;
    fn next_element_seed<S: DeserializeSeed<'de>>(
        &mut self,
        seed: S,
    ) -> Result<Option<S::Value>, Self::Error> {
        self.inner.next_element_seed(WrappedSeed {
            inner: seed,
            context: self.context,
        })
    }
    fn size_hint(&self) -> Option<usize> {
        self.inner.size_hint()
    }
}

impl<'de, A: MapAccess<'de>> MapAccess<'de> for WrappedAccess<'de, A> {
    type Error = A::Error;
    fn next_key_seed<S: DeserializeSeed<'de>>(
        &mut self,
        seed: S,
    ) -> Result<Option<S::Value>, Self::Error> {
        self.inner.next_key_seed(WrappedSeed {
            inner: seed,
            context: self.context,
        })
    }
    fn next_value_seed<S: DeserializeSeed<'de>>(
        &mut self,
        seed: S,
    ) -> Result<S::Value, Self::Error> {
        self.inner.next_value_seed(WrappedSeed {
            inner: seed,
            context: self.context,
        })
    }
}

struct WrappedEnum<'de, A> {
    inner: A,
    context: Context<'de>,
}

impl<'de, A: EnumAccess<'de>> EnumAccess<'de> for WrappedEnum<'de, A> {
    type Error = A::Error;
    type Variant = WrappedAccess<'de, A::Variant>;
    fn variant_seed<S: DeserializeSeed<'de>>(
        self,
        seed: S,
    ) -> Result<(S::Value, Self::Variant), Self::Error> {
        let (value, variant) = self.inner.variant_seed(seed)?;
        Ok((
            value,
            WrappedAccess {
                inner: variant,
                context: self.context,
            },
        ))
    }
}

impl<'de, A: VariantAccess<'de>> VariantAccess<'de> for WrappedAccess<'de, A> {
    type Error = A::Error;
    fn unit_variant(self) -> Result<(), Self::Error> {
        self.inner.unit_variant()
    }
    fn newtype_variant_seed<S: DeserializeSeed<'de>>(
        self,
        seed: S,
    ) -> Result<S::Value, Self::Error> {
        self.inner.newtype_variant_seed(WrappedSeed {
            inner: seed,
            context: self.context,
        })
    }
    fn tuple_variant<V: Visitor<'de>>(
        self,
        length: usize,
        visitor: V,
    ) -> Result<V::Value, Self::Error> {
        self.inner.tuple_variant(
            length,
            WrappedVisitor {
                inner: visitor,
                context: self.context,
            },
        )
    }
    fn struct_variant<V: Visitor<'de>>(
        self,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error> {
        self.inner.struct_variant(
            fields,
            WrappedVisitor {
                inner: visitor,
                context: self.context,
            },
        )
    }
}

#[cfg(test)]
mod tests {
    use {
        super::{DecodeError, MAX_DEPTH, deserialize},
        gluesql_core::data::{Key, Value},
        serde::{Deserialize, Serialize},
        std::collections::BTreeMap,
    };

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    enum Payload {
        Unit,
        Newtype(String),
        Tuple(u32, Option<Vec<i64>>),
        Named { entries: BTreeMap<String, Vec<u8>> },
    }

    #[test]
    fn fixed_integer_little_endian_payloads_keep_every_container_shape() {
        assert_eq!(
            deserialize::<(u32, i16)>(&[4, 3, 2, 1, 254, 255]).unwrap(),
            (0x0102_0304, -2)
        );
        for expected in [
            Payload::Unit,
            Payload::Newtype("héllo".into()),
            Payload::Tuple(73, None),
            Payload::Tuple(19, Some(vec![5, -11])),
            Payload::Named {
                entries: BTreeMap::from([("bytes".into(), vec![0, 255])]),
            },
        ] {
            let bytes = bincode::serialize(&expected).unwrap();
            assert_eq!(deserialize::<Payload>(&bytes).unwrap(), expected);
        }
    }

    #[test]
    fn decodes_numeric_keys_and_values() {
        for (key, value) in [
            (Key::I8(i8::MIN), Value::I8(i8::MAX)),
            (Key::I16(i16::MIN), Value::I16(i16::MAX)),
            (Key::I32(i32::MIN), Value::I32(i32::MAX)),
            (Key::I64(i64::MIN), Value::I64(i64::MAX)),
            (Key::I128(i128::MIN), Value::I128(i128::MAX)),
            (Key::U8(u8::MIN), Value::U8(u8::MAX)),
            (Key::U16(u16::MIN), Value::U16(u16::MAX)),
            (Key::U32(u32::MIN), Value::U32(u32::MAX)),
            (Key::U64(u64::MIN), Value::U64(u64::MAX)),
            (Key::U128(u128::MIN), Value::U128(u128::MAX)),
            (Key::F32((-1.25).into()), Value::F32(1.25)),
            (Key::F64((-2.5).into()), Value::F64(2.5)),
        ] {
            let expected = (key, vec![value]);
            let bytes = bincode::serialize(&expected).unwrap();
            let actual = deserialize::<(Key, Vec<Value>)>(&bytes).unwrap();
            assert_eq!(actual, expected);
        }
    }

    #[test]
    fn decodes_ipv4_and_ipv6_keys_and_values() {
        for address in ["192.0.2.1", "2001:db8::1"] {
            let ip = address.parse().unwrap();
            let expected = (Key::Inet(ip), vec![Value::Inet(ip)]);
            let bytes = bincode::serialize(&expected).unwrap();
            let actual = deserialize::<(Key, Vec<Value>)>(&bytes).unwrap();
            assert_eq!(actual, expected, "{address}");
        }
    }

    #[test]
    fn rejects_unconsumed_bytes() {
        assert!(matches!(deserialize::<u8>(&[1, 2]),
            Err(DecodeError::Invalid(error)) if matches!(*error,
                bincode::ErrorKind::Custom(ref message) if message == "bytes remain after the payload"
            )
        ));
    }

    #[test]
    fn rejects_truncated_fields_and_unknown_variants() {
        let valid = bincode::serialize(&Payload::Tuple(19, Some(vec![5, -11]))).unwrap();
        for end in 0..valid.len() {
            assert!(
                matches!(
                    deserialize::<Payload>(&valid[..end]),
                    Err(DecodeError::Invalid(_))
                ),
                "prefix {end}"
            );
        }
        assert!(matches!(
            deserialize::<Payload>(&u32::MAX.to_le_bytes()),
            Err(DecodeError::Invalid(_))
        ));
    }

    #[test]
    fn rejects_invalid_utf8_and_oversized_string_length() {
        let mut utf8 = 1_u64.to_le_bytes().to_vec();
        utf8.push(255);
        assert!(
            matches!(deserialize::<String>(&utf8), Err(DecodeError::Invalid(error))
                if matches!(*error, bincode::ErrorKind::InvalidUtf8Encoding(_))
            )
        );
        assert!(matches!(
            deserialize::<String>(&u64::MAX.to_le_bytes()),
            Err(DecodeError::Invalid(_))
        ));
    }

    #[test]
    fn rejects_oversized_collection_hints_before_allocating_elements() {
        let length = u64::MAX.to_le_bytes();
        assert!(
            matches!(deserialize::<Vec<()>>(&length), Err(DecodeError::Invalid(error))
                if matches!(*error, bincode::ErrorKind::Custom(ref message) if message == "sequence length exceeds payload size")
            )
        );
        assert!(
            matches!(deserialize::<BTreeMap<u8, u8>>(&length), Err(DecodeError::Invalid(error))
                if matches!(*error, bincode::ErrorKind::Custom(ref message) if message == "map length exceeds payload size")
            )
        );
    }

    #[derive(Deserialize)]
    enum Nested {
        End,
        Next(Box<Nested>),
    }

    fn nested_payload(depth: usize) -> Vec<u8> {
        std::iter::repeat_n(1_u32.to_le_bytes(), depth)
            .chain([0_u32.to_le_bytes()])
            .flatten()
            .collect()
    }

    #[test]
    fn accepts_the_depth_boundary_and_classifies_the_next_level_as_nesting_limit() {
        let mut value = deserialize::<Nested>(&nested_payload(MAX_DEPTH - 1)).unwrap();
        let mut depth = 0;
        while let Nested::Next(next) = value {
            value = *next;
            depth += 1;
        }
        assert_eq!(depth, MAX_DEPTH - 1);
        assert!(matches!(
            deserialize::<Nested>(&nested_payload(MAX_DEPTH)),
            Err(DecodeError::NestingLimit)
        ));
    }

    #[test]
    fn depth_limit_keeps_its_identity_when_the_deepest_value_is_also_invalid() {
        let mut bytes = nested_payload(MAX_DEPTH);
        bytes.truncate(bytes.len() - 4);
        bytes.extend_from_slice(&u32::MAX.to_le_bytes());
        assert!(matches!(
            deserialize::<Nested>(&bytes),
            Err(DecodeError::NestingLimit)
        ));
    }
}
