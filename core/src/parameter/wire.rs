use {super::ParameterError, crate::data::Value};

macro_rules! min_size {
    ($vec:expr, $size:expr) => {
        if $vec.len() < $size {
            return Err(Self::Error::Decode(format!("require {} bytes.", $size - 1)));
        }
    };
}

macro_rules! b_to_i {
    ($target:tt, $type:ty, $vec:expr, $size:expr) => {
        min_size!($vec, $size);
        let mut vb: [u8; $size - 1] = Default::default();
        vb.copy_from_slice(&$vec[1..$size]);
        let v: $type = <$type>::from_be_bytes(vb);
        return Ok(Value::$target(v));
    };
}

impl TryFrom<&Vec<u8>> for Value {
    type Error = ParameterError;
    fn try_from(bytes: &Vec<u8>) -> Result<Self, Self::Error> {
        // TODO: recover Value from bytes.
        if bytes.len() == 0 {
            return Err(Self::Error::Decode("Not enough data.".to_owned()));
        }
        let sig = bytes[0];
        let is_simple_type = (0b1000_0000u8 & sig) == 0;
        match (0b1000_0000u8 & sig) == 0 {
            true => {
                let (typ, sub_typ) = ((0b0111_0000u8 & sig) >> 4, (0b0000_1111u8 & sig));
                match (typ, sub_typ) {
                    (0u8, _) => {
                        return Ok(Value::Bool((0b0001 & sig) > 0));
                    }
                    (1u8, 1u8) => {
                        b_to_i!(I8, i8, bytes, 1 + 1);
                    }
                    (1u8, 2u8) => {
                        b_to_i!(I16, i16, bytes, 1 + 2);
                    }
                    (1u8, 3u8) => {
                        b_to_i!(I32, i32, bytes, 1 + 4);
                    }
                    (1u8, 4u8) => {
                        b_to_i!(I64, i64, bytes, 1 + 8);
                    }
                    (1u8, 5u8) => {
                        b_to_i!(I128, i128, bytes, 1 + 16);
                    }
                    (2u8, 1u8) => {
                        b_to_i!(U8, u8, bytes, 1 + 1);
                    }
                    (2u8, 2u8) => {
                        b_to_i!(U16, u16, bytes, 1 + 2);
                    }
                    (2u8, 3u8) => {
                        b_to_i!(U32, u32, bytes, 1 + 4);
                    }
                    (2u8, 4u8) => {
                        b_to_i!(U64, u64, bytes, 1 + 8);
                    }
                    (2u8, 5u8) => {
                        b_to_i!(U128, u128, bytes, 1 + 16);
                    }
                    (0b11u8, 1u8) => {
                        // f32
                        b_to_i!(F32, f32, bytes, 1 + 4);
                    }
                    (0b11u8, 2u8) => {
                        // f64
                        b_to_i!(F64, f64, bytes, 1 + 8);
                    }
                    _ => {
                        return Err(Self::Error::Decode(format!(
                            "unknown signed int sub type: {}.",
                            sub_typ
                        )));
                    }
                }
            }
            false => match sig & 0b0111_1111 {
                0 => {
                    return Ok(Value::Null);
                }
                _ => {}
            },
        }
        Ok(Value::I64(123))
    }
}

impl TryFrom<Vec<u8>> for Value {
    type Error = ParameterError;
    fn try_from(bytes: Vec<u8>) -> Result<Self, Self::Error> {
        // TODO: recover Value from bytes.
        (&bytes).try_into()
    }
}

impl Into<Vec<u8>> for Value {
    /// Convert a Value to Vec<u8>.
    /// The first byte is the type signature byte.
    ///
    /// Bool is all in one byte.
    /// The high 4 clear bits indicates a boolean, `0b0000`.
    /// And the rest 4 bits is it's value, 1 for true, 0 for false.
    ///   True:  0b0000_0001
    ///   False: 0b0000_0000
    ///            ^^^^       boolean type
    ///                 ^^^^  value
    ///
    /// For numeric types the type signature byte:
    ///   I8:    0b0001_0001
    ///   I16:   0b0001_0010
    ///   I32:   0b0001_0011
    ///   I64:   0b0001_0100
    ///   I128:  0b0001_0101
    ///   U8:    0b0010_0001
    ///   U16:   0b0010_0010
    ///   U32:   0b0010_0011
    ///   U64:   0b0010_0100
    ///   U128:  0b0010_0101
    ///   F32:   0b0011_0001
    ///   F64:   0b0011_0010
    ///   Dec:   0b0100_0000
    ///            ^^^^       major type
    ///                 ^^^^  minor type
    ///
    /// For other types the 1st bit in signature type is always set.
    /// The rest 7 bits indicates a type id that represents other types.
    ///  Other: 0b1000_0000
    ///           ^          always set
    ///            ^^^ ^^^^  type id
    /// Type Ids:
    ///   0: Null
    ///   1: Bytes
    ///   2: String
    ///   ...
    ///
    fn into(self) -> Vec<u8> {
        // TODO: serialize Value to bytes.
        match &self {
            Self::Bool(v) => match v {
                true => vec![0b0000_0001],
                false => vec![0b0000_0000],
            },
            Self::I8(v) => {
                vec![0b0001_0001, *v as u8]
            }
            Self::I16(v) => {
                let mut bv: Vec<u8> = vec![0b0001_0010, 0, 0];
                bv[1..].copy_from_slice(&(*v).to_be_bytes()[..]);
                bv
            }
            Self::I32(v) => {
                let mut bv: Vec<u8> = vec![0b0001_0011, 0, 0, 0, 0];
                bv[1..].copy_from_slice(&(*v).to_be_bytes()[..]);
                bv
            }
            Self::I64(v) => {
                let mut bv: Vec<u8> = vec![0b0001_0100, 0, 0, 0, 0, 0, 0, 0, 0];
                bv[1..].copy_from_slice(&(*v).to_be_bytes()[..]);
                bv
            }
            Self::I128(v) => {
                let sig: u8 = 0b0001_0101;
                let mut bv: Vec<u8> = vec![0; 17];
                bv[0] = sig;
                bv[1..].copy_from_slice(&(*v).to_be_bytes()[..]);
                bv
            }
            Self::U8(v) => {
                vec![0b0010_0001, *v]
            }
            Self::U16(v) => {
                let mut bv: Vec<u8> = vec![0b0010_0010, 0, 0];
                bv[1..].copy_from_slice(&(*v).to_be_bytes()[..]);
                bv
            }
            Self::U32(v) => {
                let mut bv: Vec<u8> = vec![0b0010_0011, 0, 0, 0, 0];
                bv[1..].copy_from_slice(&(*v).to_be_bytes()[..]);
                bv
            }
            Self::U64(v) => {
                let mut bv: Vec<u8> = vec![0b0010_0100, 0, 0, 0, 0, 0, 0, 0, 0];
                bv[1..].copy_from_slice(&(*v).to_be_bytes()[..]);
                bv
            }
            Self::U128(v) => {
                let sig: u8 = 0b0010_0101;
                let mut bv: Vec<u8> = vec![0; 1 + 16];
                bv[0] = sig;
                bv[1..].copy_from_slice(&(*v).to_be_bytes()[..]);
                bv
            }
            Self::F32(v) => {
                let sig: u8 = 0b0011_0001;
                let mut bv: Vec<u8> = vec![0; 1 + 4];
                bv[0] = sig;
                bv[1..].copy_from_slice(&(*v).to_be_bytes()[..]);
                bv
            }
            Self::F64(v) => {
                let sig: u8 = 0b0011_0010;
                let mut bv: Vec<u8> = vec![0; 1 + 8];
                bv[0] = sig;
                bv[1..].copy_from_slice(&(*v).to_be_bytes()[..]);
                bv
            }
            Self::Decimal(d) => {
                let sig: u8 = 0b0100_0000;
                let mut bv: Vec<u8> = vec![0; 1 + 16];
                bv[0] = sig;
                bv[1..].copy_from_slice(&(*d).serialize()[..]);
                bv
            }
            Self::Null => vec![0b1000_0000],
            _ => {
                vec![0]
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::data::Value;

    macro_rules! eq {
        ($bytes:expr, $value:expr) => {
            let v1: Value = TryInto::<Value>::try_into($bytes).unwrap();
            assert_eq!(v1, $value);
            let bytes: Vec<u8> = $value.try_into().unwrap();
            if bytes.len() != $bytes.len() {
                panic!(
                    "{:?} encoded to {} bytes. expects {}.",
                    $value,
                    bytes.len(),
                    $bytes.len()
                );
            } else {
                let size = bytes.len();
                for i in 0..size {
                    if bytes[i] != $bytes[i] {
                        panic!(
                            "{:?}->bytes.#{}: {}. expects {}.",
                            $value, i, bytes[i], $bytes[i]
                        );
                    }
                }
            }
        };
    }

    #[test]
    fn value_bool() {
        eq!(vec![0b0000_0001u8], Value::Bool(true));
        eq!(vec![0b0000_0000u8], Value::Bool(false));
    }

    #[test]
    fn value_ints() {
        eq!(vec![0b0001_0001u8, 17u8], Value::I8(17));
        eq!(vec![0b0001_0010u8, 0u8, 17u8], Value::I16(17));
        eq!(vec![0b0001_0011u8, 0u8, 0u8, 0u8, 17u8], Value::I32(17));
        eq!(
            vec![0b0001_0100u8, 0, 0, 0, 0, 0, 0, 0, 17u8],
            Value::I64(17)
        );
        eq!(
            vec![
                0b0001_0101u8,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                17u8
            ],
            Value::I128(17)
        );
    }

    #[test]
    fn value_unsigned_ints() {
        eq!(vec![0b0010_0001u8, 17u8], Value::U8(17));
        eq!(vec![0b0010_0010u8, 0u8, 17u8], Value::U16(17));
        eq!(vec![0b0010_0011u8, 0u8, 0u8, 0u8, 17u8], Value::U32(17));
        eq!(
            vec![0b0010_0100u8, 0, 0, 0, 0, 0, 0, 0, 17u8],
            Value::U64(17)
        );
        eq!(
            vec![
                0b0010_0101u8,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                17u8
            ],
            Value::U128(17)
        );
    }

    #[test]
    fn value_floats() {
        eq!(
            vec![0b0011_0001u8, 0x41, 0x45, 0x70, 0xa4],
            Value::F32(12.34)
        );
        eq!(
            vec![
                0b0011_0010u8,
                0xc0,
                0x28,
                0xae,
                0x14,
                0x7a,
                0xe1,
                0x47,
                0xae
            ],
            Value::F64(-12.34)
        );
    }
}
