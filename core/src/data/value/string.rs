use {crate::data::Value, std::cmp::Ordering};

impl PartialEq<str> for Value {
    fn eq(&self, other: &str) -> bool {
        match (self, other) {
            (Value::Str(l), r) => l == r,
            _ => false,
        }
    }
}

impl PartialOrd<str> for Value {
    fn partial_cmp(&self, other: &str) -> Option<Ordering> {
        match (self, other) {
            (Value::Str(l), r) => {
                let l: &str = l.as_ref();
                Some(l.cmp(r))
            }
            _ => None,
        }
    }
}

impl<T: AsRef<str>> PartialEq<T> for Value {
    fn eq(&self, other: &T) -> bool {
        PartialEq::<str>::eq(self, other.as_ref())
    }
}

impl<T: AsRef<str>> PartialOrd<T> for Value {
    fn partial_cmp(&self, other: &T) -> Option<Ordering> {
        PartialOrd::<str>::partial_cmp(self, other.as_ref())
    }
}

#[cfg(test)]
mod tests {
    use std::{borrow::Cow, cmp::Ordering};

    use crate::{data::Literal, prelude::Value};

    #[test]
    fn eq() {
        assert_eq!(Value::Str("wolf".to_owned()), "wolf");
        assert_eq!(Value::Str("wolf".to_owned()), "wolf".to_owned());
        assert_ne!(Value::I8(2), "2");

        assert_eq!(Literal::Text(Cow::Borrowed("fox")), "fox");
        assert_eq!(Literal::Text(Cow::Borrowed("fox")), "fox".to_owned());
        assert_ne!(Literal::Boolean(true), "true");
    }

    #[test]
    fn cmp() {
        macro_rules! text {
            ($text: expr) => {
                Value::Str($text.to_owned())
            };
        }
        assert_eq!(text!("wolf").partial_cmp("wolf"), Some(Ordering::Equal));
        assert_eq!(text!("apple").partial_cmp("wolf"), Some(Ordering::Less));
        assert_eq!(text!("zoo").partial_cmp("wolf"), Some(Ordering::Greater));
        assert_eq!(Value::I64(0).partial_cmp("0"), None);
    }
}
