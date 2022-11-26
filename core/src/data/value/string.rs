use {
    crate::{
        data::{Literal, Value},
        prelude::Key,
    },
    std::{borrow::Cow, cmp::Ordering},
};

impl PartialEq<String> for Value {
    fn eq(&self, other: &String) -> bool {
        match (self, other) {
            (Value::Str(l), r) => l == r,
            _ => false,
        }
    }
}

impl PartialOrd<String> for Value {
    fn partial_cmp(&self, other: &String) -> Option<Ordering> {
        match (self, other) {
            (Value::Str(l), r) => Some(l.cmp(r)),
            _ => None,
        }
    }
}

impl PartialEq<String> for Literal<'_> {
    fn eq(&self, other: &String) -> bool {
        match (self, other) {
            (&Literal::Text(Cow::Borrowed(l)), r) => l == r,
            _ => false,
        }
    }
}

impl PartialOrd<String> for Literal<'_> {
    fn partial_cmp(&self, other: &String) -> Option<Ordering> {
        match (self, other) {
            (&Literal::Text(Cow::Borrowed(l)), r) => Some(l.cmp(r)),
            _ => None,
        }
    }
}

impl From<&String> for Key {
    fn from(s: &String) -> Self {
        Key::Str(s.to_owned())
    }
}

#[cfg(test)]
mod tests {
    use std::{borrow::Cow, cmp::Ordering};

    use crate::{
        data::Literal,
        prelude::{Key, Value},
    };

    #[test]
    fn eq() {
        assert_eq!(Value::Str("wolf".to_owned()), "wolf".to_owned());
        assert_ne!(Value::I8(2), "2".to_owned());
        assert_eq!(
            Literal::Text(Cow::Borrowed(&"fox".to_owned())),
            "fox".to_owned()
        );
        assert_ne!(Literal::Boolean(true), "true".to_owned(),);
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

        assert_eq!(
            "b".to_owned().partial_cmp(&"b".to_owned()),
            Some(Ordering::Equal)
        );
        assert_eq!(
            "a".to_owned().partial_cmp(&"b".to_owned()),
            Some(Ordering::Less)
        );
        assert_eq!(
            "c".to_owned().partial_cmp(&"b".to_owned()),
            Some(Ordering::Greater)
        );

        assert_eq!(
            literal_text!("b").partial_cmp(&"b".to_owned()),
            Some(Ordering::Equal)
        );
        assert_eq!(
            literal_text!("a").partial_cmp(&"b".to_owned()),
            Some(Ordering::Less)
        );

        assert_eq!(
            literal_text!("c").partial_cmp(&"b".to_owned()),
            Some(Ordering::Greater)
        );

        assert_eq!(Literal::Boolean(true).partial_cmp(&"true".to_owned()), None);

        assert_eq!(
            text!("wolf").partial_cmp(&"wolf".to_owned()),
            Some(Ordering::Equal)
        );
        assert_eq!(
            text!("apple").partial_cmp(&"wolf".to_owned()),
            Some(Ordering::Less)
        );
        assert_eq!(
            text!("zoo").partial_cmp(&"wolf".to_owned()),
            Some(Ordering::Greater)
        );
        assert_eq!(Value::I64(0).partial_cmp(&"0".to_owned()), None);
    }

    #[test]
    fn from_key() {
        assert_eq!(Key::from(&"apple".to_owned()), Key::Str("apple".to_owned()));
    }
}
