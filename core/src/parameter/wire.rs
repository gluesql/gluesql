use crate::{
    data::Value,
    result::{Error, Result},
};

impl TryFrom<&Vec<u8>> for Value {
    type Error = Error;
    fn try_from(bytes: &Vec<u8>) -> Result<Self, Self::Error> {
        // TODO: recover Value from bytes.
        Ok(Value::I64(123))
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
    ///   0: Bytes
    ///   1: String
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
            _ => {
                vec![0]
            }
        }
    }
}
