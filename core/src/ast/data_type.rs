use serde::{Deserialize, Serialize};
use strum_macros::Display;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, Display)]
pub enum DataType {
    Boolean,
    Int8,
    Int32,
    Int,
    Int128,
    UInt8,
    UInt32,
    UInt,
    UInt128,
    Float,
    Text,
    Date,
    Timestamp,
    Time,
    Interval,
    Uuid,
    Map,
    List,
    Decimal,
}
