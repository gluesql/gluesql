use {serde::Serialize, std::fmt::Debug, thiserror::Error};

#[derive(Error, Serialize, Debug, PartialEq, Eq)]
pub enum IntervalError {
    #[error("unsupported interval range: {0} to {1}")]
    UnsupportedRange(String, String),

    #[error("cannot add between YEAR TO MONTH and HOUR TO SECOND")]
    AddBetweenYearToMonthAndHourToSecond,

    #[error("cannot subtract between YEAR TO MONTH and HOUR TO SECOND")]
    SubtractBetweenYearToMonthAndHourToSecond,

    #[error("cannot add year or month to TIME: {time} + {interval}")]
    AddYearOrMonthToTime { time: String, interval: String },

    #[error("cannot subtract year or month to TIME: {time} - {interval}")]
    SubtractYearOrMonthToTime { time: String, interval: String },

    #[error("failed to parse integer: {0}")]
    FailedToParseInteger(String),

    #[error("failed to parse decimal: {0}")]
    FailedToParseDecimal(String),

    #[error("failed to parse time: {0}")]
    FailedToParseTime(String),

    #[error("failed to parse YEAR TO MONTH (year-month, ex. 2-8): {0}")]
    FailedToParseYearToMonth(String),

    #[error("failed to parse DAY TO HOUR (day hour, ex. 1 23): {0}")]
    FailedToParseDayToHour(String),

    #[error("failed to parse DAY TO MINUTE (day hh:mm, ex. 1 12:34): {0}")]
    FailedToParseDayToMinute(String),

    #[error("failed to parse DAY TO SECOND (day hh:mm:ss, ex. 1 12:34:55): {0}")]
    FailedToParseDayToSecond(String),

    #[error("date overflow: {year}-{month}")]
    DateOverflow { year: i32, month: i32 },

    #[error("failed to get extract from interval")]
    FailedToExtract,

    #[error("unreachable")]
    Unreachable,
}
