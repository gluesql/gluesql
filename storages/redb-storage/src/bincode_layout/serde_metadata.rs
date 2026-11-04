//! Read the metadata emitted by Serde derives without constructing sample values.

use serde::de::{self, DeserializeOwned, DeserializeSeed, EnumAccess, VariantAccess, Visitor};

type Error = de::value::Error;
const CAPTURED: &str = "layout metadata captured";

#[derive(Debug, PartialEq, Eq)]
pub(super) enum Shape {
    Unit,
    Newtype,
    Tuple(usize),
    Struct(&'static [&'static str]),
}

// Stopping before field values is intentional: the metadata is all we need.
fn captured<T>(result: &Result<T, Error>) {
    assert!(
        matches!(result, Err(error) if error.to_string() == CAPTURED),
        "expected the metadata reader to stop before reading field values"
    );
}

struct EnumProbe<'a> {
    index: u32,
    names: &'a mut Option<&'static [&'static str]>,
    shape: &'a mut Option<Shape>,
}

impl<'de> de::Deserializer<'de> for EnumProbe<'_> {
    type Error = Error;

    fn deserialize_any<V: Visitor<'de>>(self, _: V) -> Result<V::Value, Error> {
        Err(de::Error::custom("expected an externally tagged enum"))
    }

    fn deserialize_enum<V: Visitor<'de>>(
        self,
        _: &'static str,
        names: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Error> {
        *self.names = Some(names);
        visitor.visit_enum(self)
    }

    serde::forward_to_deserialize_any! {
        bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string
        bytes byte_buf option unit unit_struct newtype_struct seq tuple
        tuple_struct map struct identifier ignored_any
    }
}

impl<'de> EnumAccess<'de> for EnumProbe<'_> {
    type Error = Error;
    type Variant = Self;

    fn variant_seed<T: DeserializeSeed<'de>>(self, seed: T) -> Result<(T::Value, Self), Error> {
        let variant = seed.deserialize(de::value::U32Deserializer::<Error>::new(self.index))?;
        Ok((variant, self))
    }
}

impl<'de> VariantAccess<'de> for EnumProbe<'_> {
    type Error = Error;

    fn unit_variant(self) -> Result<(), Error> {
        *self.shape = Some(Shape::Unit);
        Err(de::Error::custom(CAPTURED))
    }

    fn newtype_variant_seed<T: DeserializeSeed<'de>>(self, _: T) -> Result<T::Value, Error> {
        *self.shape = Some(Shape::Newtype);
        Err(de::Error::custom(CAPTURED))
    }

    fn tuple_variant<V: Visitor<'de>>(self, len: usize, _: V) -> Result<V::Value, Error> {
        *self.shape = Some(Shape::Tuple(len));
        Err(de::Error::custom(CAPTURED))
    }

    fn struct_variant<V: Visitor<'de>>(
        self,
        fields: &'static [&'static str],
        _: V,
    ) -> Result<V::Value, Error> {
        *self.shape = Some(Shape::Struct(fields));
        Err(de::Error::custom(CAPTURED))
    }
}

fn enum_variant<T: DeserializeOwned>(index: u32) -> (&'static [&'static str], Shape) {
    let mut names = None;
    let mut shape = None;
    captured(&T::deserialize(EnumProbe {
        index,
        names: &mut names,
        shape: &mut shape,
    }));
    (
        names.expect("enum variant names"),
        shape.expect("enum variant shape"),
    )
}

pub(super) fn enum_layout<T: DeserializeOwned>() -> Vec<(&'static str, Shape)> {
    let (names, first) = enum_variant::<T>(0);
    let mut variants = vec![(names[0], first)];
    for (index, name) in names.iter().enumerate().skip(1) {
        let index = u32::try_from(index).expect("Serde enum variant index fits u32");
        let (_, shape) = enum_variant::<T>(index);
        variants.push((*name, shape));
    }
    variants
}

struct StructProbe<'a>(&'a mut Option<Shape>);

impl<'de> de::Deserializer<'de> for StructProbe<'_> {
    type Error = Error;

    fn deserialize_any<V: Visitor<'de>>(self, _: V) -> Result<V::Value, Error> {
        Err(de::Error::custom("expected a struct"))
    }

    fn deserialize_struct<V: Visitor<'de>>(
        self,
        _: &'static str,
        fields: &'static [&'static str],
        _: V,
    ) -> Result<V::Value, Error> {
        *self.0 = Some(Shape::Struct(fields));
        Err(de::Error::custom(CAPTURED))
    }

    fn deserialize_newtype_struct<V: Visitor<'de>>(
        self,
        _: &'static str,
        _: V,
    ) -> Result<V::Value, Error> {
        *self.0 = Some(Shape::Newtype);
        Err(de::Error::custom(CAPTURED))
    }

    serde::forward_to_deserialize_any! {
        bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string
        bytes byte_buf option unit unit_struct seq tuple tuple_struct map enum identifier ignored_any
    }
}

pub(super) fn struct_layout<T: DeserializeOwned>() -> Shape {
    let mut shape = None;
    captured(&T::deserialize(StructProbe(&mut shape)));
    shape.expect("struct shape")
}
