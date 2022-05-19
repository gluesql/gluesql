use serde::{Deserialize, Serialize};
use strum_macros::Display;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, Display)]
pub enum DataType {
    Boolean,
    Int8,
    Int,
    Int128,
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
