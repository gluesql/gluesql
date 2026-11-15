use crate::data::Value;

/// A typed value for native parameters and `value()` expressions.
/// Strings are stored as text without being parsed as SQL.
#[derive(Clone, Debug)]
pub struct ValueNode(pub(super) Value);

impl From<Value> for ValueNode {
    fn from(value: Value) -> Self {
        Self(value)
    }
}

impl From<bool> for ValueNode {
    fn from(value: bool) -> Self {
        Self(Value::Bool(value))
    }
}

impl From<i8> for ValueNode {
    fn from(value: i8) -> Self {
        Self(Value::I8(value))
    }
}

impl From<i16> for ValueNode {
    fn from(value: i16) -> Self {
        Self(Value::I16(value))
    }
}

impl From<i32> for ValueNode {
    fn from(value: i32) -> Self {
        Self(Value::I32(value))
    }
}

impl From<i64> for ValueNode {
    fn from(value: i64) -> Self {
        Self(Value::I64(value))
    }
}

impl From<i128> for ValueNode {
    fn from(value: i128) -> Self {
        Self(Value::I128(value))
    }
}

impl From<u8> for ValueNode {
    fn from(value: u8) -> Self {
        Self(Value::U8(value))
    }
}

impl From<u16> for ValueNode {
    fn from(value: u16) -> Self {
        Self(Value::U16(value))
    }
}

impl From<u32> for ValueNode {
    fn from(value: u32) -> Self {
        Self(Value::U32(value))
    }
}

impl From<u64> for ValueNode {
    fn from(value: u64) -> Self {
        Self(Value::U64(value))
    }
}

impl From<u128> for ValueNode {
    fn from(value: u128) -> Self {
        Self(Value::U128(value))
    }
}

impl From<f32> for ValueNode {
    fn from(value: f32) -> Self {
        Self(Value::F32(value))
    }
}

impl From<f64> for ValueNode {
    fn from(value: f64) -> Self {
        Self(Value::F64(value))
    }
}

impl From<String> for ValueNode {
    fn from(value: String) -> Self {
        Self(Value::Str(value))
    }
}

impl From<&str> for ValueNode {
    fn from(value: &str) -> Self {
        Self(Value::Str(value.to_owned()))
    }
}

impl From<Vec<u8>> for ValueNode {
    fn from(value: Vec<u8>) -> Self {
        Self(Value::Bytea(value))
    }
}

impl From<&[u8]> for ValueNode {
    fn from(value: &[u8]) -> Self {
        Self(Value::Bytea(value.to_vec()))
    }
}

#[cfg(test)]
mod tests {
    use crate::{data::Value, query_builder::ValueNode};

    #[test]
    fn preserves_primitive_types() {
        assert_eq!(ValueNode::from(true).0, Value::Bool(true));

        assert_eq!(ValueNode::from(-8_i8).0, Value::I8(-8));
        assert_eq!(ValueNode::from(-16_i16).0, Value::I16(-16));
        assert_eq!(ValueNode::from(-32_i32).0, Value::I32(-32));
        assert_eq!(ValueNode::from(-64_i64).0, Value::I64(-64));
        assert_eq!(ValueNode::from(-128_i128).0, Value::I128(-128));

        assert_eq!(ValueNode::from(8_u8).0, Value::U8(8));
        assert_eq!(ValueNode::from(16_u16).0, Value::U16(16));
        assert_eq!(ValueNode::from(32_u32).0, Value::U32(32));
        assert_eq!(ValueNode::from(64_u64).0, Value::U64(64));
        assert_eq!(ValueNode::from(128_u128).0, Value::U128(128));

        assert_eq!(ValueNode::from(0.1_f32).0, Value::F32(0.1));
        assert_eq!(ValueNode::from(0.1_f64).0, Value::F64(0.1));
    }

    #[test]
    fn accepts_owned_and_borrowed_text_and_bytes() {
        assert_eq!(
            ValueNode::from("name + 1").0,
            Value::Str("name + 1".to_owned())
        );
        assert_eq!(
            ValueNode::from("owned".to_owned()).0,
            Value::Str("owned".to_owned())
        );

        assert_eq!(ValueNode::from(vec![0, 255]).0, Value::Bytea(vec![0, 255]));
        assert_eq!(
            ValueNode::from(b"bytes".as_slice()).0,
            Value::Bytea(b"bytes".to_vec())
        );
    }

    #[test]
    fn accepts_existing_values() {
        let null = ValueNode::from(Value::Null);
        assert_eq!(null.0, Value::Null);

        let list = ValueNode::from(Value::List(vec![Value::I8(1), Value::Null]));
        assert_eq!(list.0, Value::List(vec![Value::I8(1), Value::Null]));
    }
}
