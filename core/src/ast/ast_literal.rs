use {
    bigdecimal::BigDecimal,
    serde::{Deserialize, Serialize},
};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum AstLiteral {
    Boolean(bool),
    Number(BigDecimal),
    QuotedString(String),
    Interval {
        value: String,
        leading_field: Option<DateTimeField>,
        last_field: Option<DateTimeField>,
    },
    Null,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DateTimeField {
    Year,
    Month,
    Day,
    Hour,
    Minute,
    Second,
    Century,
    Decade,
    Week,
    Dow, //day of week
    Doy, //day of year
    Epoch,
    Isodow,
    Isoyear,
    Julian,
    Microseconds,
    Millenium,
    Milliseconds,
    Quarter,
    Timezone,
    TimezoneHour,
    TimezoneMinute,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TrimWhereField {
    Both,
    Leading,
    Trailing,
}
