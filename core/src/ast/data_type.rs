use serde::{Deserialize, Serialize};
use strum_macros::Display;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, Display)]
#[strum(serialize_all = "SCREAMING_SNAKE_CASE")]
pub enum DataType {
    Boolean,
    Int8,
    Int,
    Float,
    Text,
    Bytea,
    Date,
    Timestamp,
    Time,
    Interval,
    Uuid,
    Map,
    List,
    Decimal(Option<u64>, Option<u64>),
}
