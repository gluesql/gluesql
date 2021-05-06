mod error;
mod primitive;
mod string;

use {
    crate::result::Result,
    chrono::{Datelike, Duration, NaiveDate, NaiveDateTime, NaiveTime, Timelike},
    core::str::FromStr,
    rust_decimal::{prelude::ToPrimitive, Decimal},
    serde::{Deserialize, Serialize},
    sqlparser::ast::DateTimeField,
    std::{cmp::Ordering, fmt::Debug},
};

pub use error::IntervalError;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Interval {
    Month(i32),
    Microsecond(i64),
}

impl PartialOrd<Interval> for Interval {
    fn partial_cmp(&self, other: &Interval) -> Option<Ordering> {
        match (self, other) {
            (Interval::Month(l), Interval::Month(r)) => Some(l.cmp(r)),
            (Interval::Microsecond(l), Interval::Microsecond(r)) => Some(l.cmp(r)),
            _ => None,
        }
    }
}

const SECOND: i64 = 1_000_000;
const MINUTE: i64 = 60 * SECOND;
const HOUR: i64 = 3600 * SECOND;
const DAY: i64 = 24 * HOUR;

impl Interval {
    pub fn unary_minus(&self) -> Self {
        match self {
            Interval::Month(v) => Interval::Month(-v),
            Interval::Microsecond(v) => Interval::Microsecond(-v),
        }
    }

    pub fn add(&self, other: &Interval) -> Result<Self> {
        use Interval::*;

        match (self, other) {
            (Month(l), Month(r)) => Ok(Month(l + r)),
            (Microsecond(l), Microsecond(r)) => Ok(Microsecond(l + r)),
            _ => Err(IntervalError::AddBetweenYearToMonthAndHourToSecond.into()),
        }
    }

    pub fn subtract(&self, other: &Interval) -> Result<Self> {
        use Interval::*;

        match (self, other) {
            (Month(l), Month(r)) => Ok(Month(l - r)),
            (Microsecond(l), Microsecond(r)) => Ok(Microsecond(l - r)),
            _ => Err(IntervalError::SubtractBetweenYearToMonthAndHourToSecond.into()),
        }
    }

    pub fn add_date(&self, date: &NaiveDate) -> Result<NaiveDateTime> {
        self.add_timestamp(&date.and_hms(0, 0, 0))
    }

    pub fn subtract_from_date(&self, date: &NaiveDate) -> Result<NaiveDateTime> {
        self.subtract_from_timestamp(&date.and_hms(0, 0, 0))
    }

    pub fn add_timestamp(&self, timestamp: &NaiveDateTime) -> Result<NaiveDateTime> {
        match self {
            Interval::Month(n) => {
                let month = timestamp.month() as i32 + n;

                let year = timestamp.year() + month / 12;
                let month = month % 12;

                timestamp
                    .with_year(year)
                    .map(|d| d.with_month(month as u32))
                    .flatten()
                    .ok_or_else(|| IntervalError::DateOverflow { year, month }.into())
            }
            Interval::Microsecond(n) => Ok(*timestamp + Duration::microseconds(*n)),
        }
    }

    pub fn subtract_from_timestamp(&self, timestamp: &NaiveDateTime) -> Result<NaiveDateTime> {
        match self {
            Interval::Month(n) => {
                let months = timestamp.year() * 12 + timestamp.month() as i32 - n;

                let year = months / 12;
                let month = months % 12;

                timestamp
                    .with_year(year)
                    .map(|d| d.with_month(month as u32))
                    .flatten()
                    .ok_or_else(|| IntervalError::DateOverflow { year, month }.into())
            }
            Interval::Microsecond(n) => Ok(*timestamp - Duration::microseconds(*n)),
        }
    }

    pub fn years(years: i32) -> Self {
        Interval::Month(12 * years)
    }

    pub fn months(months: i32) -> Self {
        Interval::Month(months)
    }

    pub fn days(days: i32) -> Self {
        Interval::Microsecond(days as i64 * DAY)
    }

    pub fn hours(hours: i32) -> Self {
        Interval::Microsecond(hours as i64 * HOUR)
    }

    pub fn minutes(minutes: i32) -> Self {
        Interval::Microsecond(minutes as i64 * MINUTE)
    }

    pub fn seconds(seconds: i64) -> Self {
        Interval::Microsecond(seconds * SECOND)
    }

    pub fn milliseconds(milliseconds: i64) -> Self {
        Interval::Microsecond(milliseconds * 1_000)
    }

    pub fn microseconds(microseconds: i64) -> Self {
        Interval::Microsecond(microseconds)
    }

    pub fn try_from_literal(
        value: &str,
        leading_field: Option<&DateTimeField>,
        last_field: Option<&DateTimeField>,
    ) -> Result<Self> {
        use DateTimeField::*;

        let parse_integer = |v: &str| {
            v.parse::<i32>()
                .map_err(|_| IntervalError::FailedToParseInteger(value.to_owned()).into())
        };

        let parse_decimal = |duration: i64| {
            let parsed = Decimal::from_str(value)
                .map_err(|_| IntervalError::FailedToParseDecimal(value.to_owned()))?;

            (parsed * Decimal::from(duration))
                .to_i64()
                .ok_or_else(|| IntervalError::FailedToParseDecimal(value.to_owned()).into())
                .map(Interval::Microsecond)
        };

        let parse_time = |value| {
            let time = NaiveTime::from_str(value)
                .map_err(|_| IntervalError::FailedToParseTime(value.to_owned()))?;
            let msec = time.hour() as i64 * HOUR
                + time.minute() as i64 * MINUTE
                + time.second() as i64 * SECOND
                + time.nanosecond() as i64 / 1000;

            Ok(Interval::Microsecond(msec))
        };

        match (leading_field, last_field) {
            (Some(Year), None) => parse_integer(value).map(Interval::years),
            (Some(Month), None) => parse_integer(value).map(Interval::months),
            (Some(Day), None) => parse_decimal(DAY),
            (Some(Hour), None) => parse_decimal(HOUR),
            (Some(Minute), None) => parse_decimal(MINUTE),
            (Some(Second), None) => parse_decimal(SECOND),
            (Some(Year), Some(Month)) => {
                let nums = value
                    .split('-')
                    .map(parse_integer)
                    .collect::<Result<Vec<_>>>()?;

                match (nums.get(0), nums.get(1)) {
                    (Some(years), Some(months)) => Ok(Interval::months(12 * years + months)),
                    _ => Err(IntervalError::FailedToParseYearToMonth(value.to_owned()).into()),
                }
            }
            (Some(Day), Some(Hour)) => {
                let nums = value
                    .split(' ')
                    .map(parse_integer)
                    .collect::<Result<Vec<_>>>()?;

                match (nums.get(0), nums.get(1)) {
                    (Some(days), Some(hours)) => Ok(Interval::hours(24 * days + hours)),
                    _ => Err(IntervalError::FailedToParseDayToHour(value.to_owned()).into()),
                }
            }
            (Some(Day), Some(Minute)) => {
                let nums = value.split(' ').collect::<Vec<_>>();

                match (nums.get(0), nums.get(1)) {
                    (Some(days), Some(time)) => {
                        let days = parse_integer(days)?;
                        let time = NaiveTime::from_str(&format!("{}:00", time)).unwrap();
                        let minutes = time.hour() * 60 + time.minute();

                        Interval::days(days).add(&Interval::minutes(minutes as i32))
                    }
                    _ => Err(IntervalError::FailedToParseDayToMinute(value.to_owned()).into()),
                }
            }
            (Some(Day), Some(Second)) => {
                let nums = value.split(' ').collect::<Vec<_>>();

                match (nums.get(0), nums.get(1)) {
                    (Some(days), Some(time)) => {
                        let days = parse_integer(days)?;

                        Interval::days(days).add(&parse_time(&time)?)
                    }
                    _ => Err(IntervalError::FailedToParseDayToSecond(value.to_owned()).into()),
                }
            }
            (Some(Hour), Some(Minute)) => parse_time(&format!("{}:00", value)),
            (Some(Hour), Some(Second)) => parse_time(value),
            (Some(Minute), Some(Second)) => parse_time(&format!("00:{}", value)),
            (Some(from), Some(to)) => Err(IntervalError::UnsupportedRange(
                format!("{:?}", from),
                format!("{:?}", to),
            )
            .into()),
            (None, _) => Err(IntervalError::Unreachable.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Interval, IntervalError};
    use sqlparser::ast::DateTimeField;

    #[test]
    fn arithmetic() {
        use chrono::NaiveDate;
        use Interval::*;

        macro_rules! test {
            ($op: ident $a: expr, $b: expr => $c: expr) => {
                assert_eq!($a.$op(&$b), Ok($c));
            };
        }

        let date = |y, m, d| NaiveDate::from_ymd(y, m, d);

        assert_eq!(Month(1).unary_minus(), Month(-1));
        assert_eq!(Microsecond(1).unary_minus(), Microsecond(-1));
        assert_eq!(
            Month(2).add_date(&date(2021, 11, 11)),
            Ok(date(2022, 1, 11).and_hms(0, 0, 0))
        );
        assert_eq!(
            Interval::hours(30).add_date(&date(2021, 11, 11)),
            Ok(date(2021, 11, 12).and_hms(6, 0, 0))
        );
        assert_eq!(
            Interval::years(999_999).add_date(&date(2021, 11, 11)),
            Err(IntervalError::DateOverflow {
                year: 1_002_020,
                month: 11,
            }
            .into())
        );
        assert_eq!(
            Month(2).subtract_from_date(&date(2021, 11, 11)),
            Ok(date(2021, 9, 11).and_hms(0, 0, 0))
        );
        assert_eq!(
            Month(14).subtract_from_date(&date(2021, 11, 11)),
            Ok(date(2020, 9, 11).and_hms(0, 0, 0))
        );
        assert_eq!(
            Interval::hours(30).subtract_from_date(&date(2021, 11, 11)),
            Ok(date(2021, 11, 9).and_hms(18, 0, 0))
        );
        assert_eq!(
            Interval::years(999_999).subtract_from_date(&date(2021, 11, 11)),
            Err(IntervalError::DateOverflow {
                year: -997977,
                month: -1,
            }
            .into())
        );
        assert_eq!(
            Interval::minutes(2).add_timestamp(&date(2021, 11, 11).and_hms(12, 3, 1)),
            Ok(date(2021, 11, 11).and_hms(12, 5, 1))
        );
        assert_eq!(
            Interval::hours(30).add_timestamp(&date(2021, 11, 11).and_hms(0, 30, 0)),
            Ok(date(2021, 11, 12).and_hms(6, 30, 0))
        );
        assert_eq!(
            Interval::years(999_999).add_timestamp(&date(2021, 11, 11).and_hms(1, 1, 1)),
            Err(IntervalError::DateOverflow {
                year: 1_002_020,
                month: 11,
            }
            .into())
        );
        assert_eq!(
            Month(2).subtract_from_timestamp(&date(2021, 11, 11).and_hms(1, 3, 59)),
            Ok(date(2021, 9, 11).and_hms(1, 3, 59))
        );
        assert_eq!(
            Month(14).subtract_from_timestamp(&date(2021, 11, 11).and_hms(23, 1, 1)),
            Ok(date(2020, 9, 11).and_hms(23, 1, 1))
        );
        assert_eq!(
            Interval::seconds(30).subtract_from_timestamp(&date(2021, 11, 11).and_hms(0, 0, 0)),
            Ok(date(2021, 11, 10).and_hms(23, 59, 30))
        );
        assert_eq!(
            Interval::years(999_999).subtract_from_timestamp(&date(2021, 11, 11).and_hms(0, 0, 0)),
            Err(IntervalError::DateOverflow {
                year: -997977,
                month: -1,
            }
            .into())
        );

        test!(add      Month(1), Month(2) => Month(3));
        test!(subtract Month(1), Month(2) => Month(-1));

        test!(add      Microsecond(1), Microsecond(2) => Microsecond(3));
        test!(subtract Microsecond(1), Microsecond(2) => Microsecond(-1));
    }

    #[test]
    fn try_from_literal() {
        macro_rules! test {
            ($value: expr, $datetime: ident => $expected_value: expr, $duration: ident) => {
                let interval =
                    Interval::try_from_literal($value, Some(&DateTimeField::$datetime), None);

                assert_eq!(interval, Ok(Interval::$duration($expected_value)));
            };
            ($value: expr, $from: ident to $to: ident => $expected_value: expr, $duration: ident) => {
                let interval = Interval::try_from_literal(
                    $value,
                    Some(&DateTimeField::$from),
                    Some(&DateTimeField::$to),
                );

                assert_eq!(interval, Ok(Interval::$duration($expected_value)));
            };
        }

        test!("11",   Year   => 11,  years);
        test!("-11",  Year   => -11, years);
        test!("18",   Month  => 18,  months);
        test!("-19",  Month  => -19, months);
        test!("2",    Day    => 2,   days);
        test!("1.5",  Day    => 36,  hours);
        test!("-1.5", Day    => -36, hours);
        test!("2.5",  Hour   => 150, minutes);
        test!("1",    Hour   => 60,  minutes);
        test!("-1",   Hour   => -60, minutes);
        test!("35",   Minute => 35,  minutes);
        test!("-35",  Minute => -35, minutes);
        test!("10.5", Minute => 630, seconds);
        test!("10",   Second => 10,  seconds);
        test!("10.5", Second => 10_500_000, microseconds);

        test!("10-2", Year to Month => 122, months);
        test!("2 12", Day to Hour => 60, hours);
        test!("1 01:30", Day to Minute => 60 * 24 + 90, minutes);
        test!("1 01:30:40", Day to Second => (60 * 24 + 90) * 60 + 40, seconds);
        test!("3 02:30:40.1234", Day to Second =>
            (((3 * 24 + 2) * 60 + 30) * 60 + 40) * 1_000_000 + 123_400, microseconds);
        test!("12:34", Hour to Minute => 12 * 60 + 34, minutes);
        test!("12:34:56", Hour to Second => (12 * 60 + 34) * 60 + 56, seconds);
        test!("12:34:56.1234", Hour to Second => ((12 * 60 + 34) * 60 + 56) * 1_000_000 + 123_400, microseconds);
        test!("34:56.1234", Minute to Second => (34 * 60 + 56) * 1_000_000 + 123_400, microseconds);
    }
}
