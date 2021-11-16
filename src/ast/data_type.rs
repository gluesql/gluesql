use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DataType {
    Boolean,
    Int8,
    Int,
    Float,
    Text,
    Date,
    Timestamp,
    Time,
    Interval,
    Uuid,
    Map,
    List,
}
