use {
    crate::{
        ast::AstLiteral,
        ast_builder::AstBuilderError,
        result::{Error, Result},
    },
    bigdecimal::BigDecimal,
    std::{borrow::Cow, str::FromStr},
};

#[derive(Clone, Debug)]
pub enum NumericNode<'a> {
    I8(i8),
    I16(i16),
    I32(i32),
    I64(i64),
    U8(u8),
    U16(u16),
    U32(u32),
    U64(u64),
    F32(f32),
    F64(f64),
    Str(Cow<'a, str>),
}

macro_rules! impl_from {
    ($type: path, $name: ident) => {
        impl<'a> From<$type> for NumericNode<'a> {
            fn from(v: $type) -> Self {
                NumericNode::$name(v)
            }
        }
    };
}

impl_from!(i8, I8);
impl_from!(i16, I16);
impl_from!(i32, I32);
impl_from!(i64, I64);
impl_from!(u8, U8);
impl_from!(u16, U16);
impl_from!(u32, U32);
impl_from!(u64, U64);
impl_from!(f32, F32);
impl_from!(f64, F64);

impl<'a> From<String> for NumericNode<'a> {
    fn from(v: String) -> Self {
        Self::Str(Cow::Owned(v))
    }
}

impl<'a> From<&'a str> for NumericNode<'a> {
    fn from(v: &'a str) -> Self {
        Self::Str(Cow::Borrowed(v))
    }
}

impl<'a> TryFrom<NumericNode<'a>> for AstLiteral {
    type Error = Error;

    fn try_from(node: NumericNode<'a>) -> Result<Self> {
        match node {
            NumericNode::I8(v) => Ok(AstLiteral::Number(v.into())),
            NumericNode::I16(v) => Ok(AstLiteral::Number(v.into())),
            NumericNode::I32(v) => Ok(AstLiteral::Number(v.into())),
            NumericNode::I64(v) => Ok(AstLiteral::Number(v.into())),
            NumericNode::U8(v) => Ok(AstLiteral::Number(v.into())),
            NumericNode::U16(v) => Ok(AstLiteral::Number(v.into())),
            NumericNode::U32(v) => Ok(AstLiteral::Number(v.into())),
            NumericNode::U64(v) => Ok(AstLiteral::Number(v.into())),
            NumericNode::F32(v) => BigDecimal::try_from(v)
                .map_err(|_| AstBuilderError::FailedToParseNumeric(v.to_string()).into())
                .map(AstLiteral::Number),
            NumericNode::F64(v) => BigDecimal::try_from(v)
                .map_err(|_| AstBuilderError::FailedToParseNumeric(v.to_string()).into())
                .map(AstLiteral::Number),
            NumericNode::Str(v) => BigDecimal::from_str(&v)
                .map_err(|_| AstBuilderError::FailedToParseNumeric(v.into_owned()).into())
                .map(AstLiteral::Number),
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        crate::{
            ast::AstLiteral,
            ast_builder::{AstBuilderError, NumericNode},
        },
        bigdecimal::BigDecimal,
        std::str::FromStr,
    };

    #[test]
    fn numeric() {
        let num = |n| Ok(AstLiteral::Number(BigDecimal::from_str(n).unwrap()));

        assert_eq!(NumericNode::from(1_i8).try_into(), num("1"));
        assert_eq!(NumericNode::from(1_i16).try_into(), num("1"));
        assert_eq!(NumericNode::from(1_i32).try_into(), num("1"));
        assert_eq!(NumericNode::from(1_i64).try_into(), num("1"));
        assert_eq!(NumericNode::from(1_u8).try_into(), num("1"));
        assert_eq!(NumericNode::from(1_u16).try_into(), num("1"));
        assert_eq!(NumericNode::from(1_u32).try_into(), num("1"));
        assert_eq!(NumericNode::from(1_u64).try_into(), num("1"));
        assert_eq!(NumericNode::from(1.50_f32).try_into(), num("1.50"));
        assert_eq!(NumericNode::from(4.125_f64).try_into(), num("4.125"));
        assert_eq!(NumericNode::from("123.456").try_into(), num("123.456"));
        assert_eq!(NumericNode::from("1.6".to_owned()).try_into(), num("1.6"));

        assert_eq!(
            AstLiteral::try_from(NumericNode::from(f32::NAN)),
            Err(AstBuilderError::FailedToParseNumeric(f32::NAN.to_string()).into()),
        );
        assert_eq!(
            AstLiteral::try_from(NumericNode::from(f64::NAN)),
            Err(AstBuilderError::FailedToParseNumeric(f64::NAN.to_string()).into()),
        );
        assert_eq!(
            AstLiteral::try_from(NumericNode::from("not a number")),
            Err(AstBuilderError::FailedToParseNumeric("not a number".to_owned()).into()),
        );
    }
}
