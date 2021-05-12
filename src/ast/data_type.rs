use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DataType {
    Boolean,
    Int,
    Float,
    Text,
    Date,
    Timestamp,
    Time,
    Interval,
}
