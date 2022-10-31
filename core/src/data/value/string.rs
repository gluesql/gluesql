use std::{borrow::Cow, cmp::Ordering};

use crate::data::{Literal, Value};

impl PartialEq<&str> for Value {
    fn eq(&self, other: &&str) -> bool {
        match (self, other) {
            (Value::Str(l), r) => &l[..] == *r,
            _ => false,
        }
    }
}

impl PartialOrd<&str> for Value {
    fn partial_cmp(&self, other: &&str) -> Option<Ordering> {
        match (self, other) {
            (Value::Str(l), r) => Some(l[..].cmp(r)),
            _ => None,
        }
    }
}

impl PartialEq<&str> for Literal<'_> {
    fn eq(&self, other: &&str) -> bool {
        match (self, other) {
            (&Literal::Text(Cow::Borrowed(l)), &r) => l[..] == *r,
            _ => false,
        }
    }
}

impl PartialOrd<&str> for Literal<'_> {
    fn partial_cmp(&self, other: &&str) -> Option<Ordering> {
        match (self, other) {
            (&Literal::Text(Cow::Borrowed(l)), r) => Some(l[..].cmp(r)),
            _ => None,
        }
    }
}

impl From<&str> for Value {
    fn from(str_slice: &str) -> Self {
        Value::Str(str_slice.to_owned())
    }
}

#[cfg(test)]
mod tests {
    use std::{borrow::Cow, cmp::Ordering};

    use crate::{data::Literal, prelude::Value};

    #[test]
    fn eq() {
        assert_eq!(Value::Str("wolf".to_owned()), "wolf");
        assert_eq!(Value::Str("wolf".to_owned()) == "wow", false);
        assert_eq!(Literal::Text(Cow::Borrowed(&"fox".to_owned())), "fox");
        assert_eq!(
            Literal::Text(Cow::Borrowed(&"fox".to_owned())) == "foo",
            false
        );
    }

    #[test]
    fn cmp() {
        macro_rules! literal_text {
            ($text: expr) => {
                Literal::Text(Cow::Borrowed(&$text.to_owned()))
            };
        }

        macro_rules! text {
            ($text: expr) => {
                Value::Str($text.to_owned())
            };
        }

        assert_eq!("b".partial_cmp("b"), Some(Ordering::Equal));
        assert_eq!("a".partial_cmp("b"), Some(Ordering::Less));
        assert_eq!("c".partial_cmp("b"), Some(Ordering::Greater));

        assert_eq!(literal_text!("b").partial_cmp(&"b"), Some(Ordering::Equal));
        assert_eq!(literal_text!("a").partial_cmp(&"b"), Some(Ordering::Less));
        assert_eq!(
            literal_text!("c").partial_cmp(&"b"),
            Some(Ordering::Greater)
        );
        assert_eq!(Literal::Boolean(true).partial_cmp(&"true"), None);

        assert_eq!(text!("wolf").partial_cmp(&"wolf"), Some(Ordering::Equal));
        assert_eq!(text!("apple").partial_cmp(&"wolf"), Some(Ordering::Less));
        assert_eq!(text!("zoo").partial_cmp(&"wolf"), Some(Ordering::Greater));
        assert_eq!(Value::I64(0).partial_cmp(&"0"), None);
    }

    #[test]
    fn from_str() {
        assert_eq!(Value::Str("meat".to_owned()), Value::from("meat"));
    }
}
