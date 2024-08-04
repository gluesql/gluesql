mod error;
mod primitive;
mod string;

pub use error::IntervalError;
use {
    super::Value,
    crate::{ast::DateTimeField, result::Result},
    chrono::{Datelike, Duration, NaiveDate, NaiveDateTime, NaiveTime, Timelike},
    core::str::FromStr,
    rust_decimal::{prelude::ToPrimitive, Decimal},
    serde::{Deserialize, Serialize},
    std::{cmp::Ordering, fmt::Debug},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Interval {
    Month(i32),
    Microsecond(i64),
}

impl Ord for Interval {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (Interval::Month(l), Interval::Month(r)) => l.cmp(r),
            (Interval::Microsecond(l), Interval::Microsecond(r)) => l.cmp(r),
            (Interval::Month(_), Interval::Microsecond(_)) => Ordering::Greater,
            (Interval::Microsecond(_), Interval::Month(_)) => Ordering::Less,
        }
    }
}

impl PartialOrd<Interval> for Interval {
    fn partial_cmp(&self, other: &Interval) -> Option<Ordering> {
        Some(self.cmp(other))
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
        self.add_timestamp(
            &date
                .and_hms_opt(0, 0, 0)
                .ok_or_else(|| IntervalError::FailedToParseTime(date.to_string()))?,
        )
    }

    pub fn subtract_from_date(&self, date: &NaiveDate) -> Result<NaiveDateTime> {
        self.subtract_from_timestamp(
            &date
                .and_hms_opt(0, 0, 0)
                .ok_or_else(|| IntervalError::FailedToParseTime(date.to_string()))?,
        )
    }

    pub fn add_timestamp(&self, timestamp: &NaiveDateTime) -> Result<NaiveDateTime> {
        match self {
            Interval::Month(n) => {
                let month = timestamp.month() as i32 + n;

                let year = timestamp.year() + month / 12;
                let month = month % 12;

                timestamp
                    .with_year(year)
                    .and_then(|d| d.with_month(month as u32))
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
                    .and_then(|d| d.with_month(month as u32))
                    .ok_or_else(|| IntervalError::DateOverflow { year, month }.into())
            }
            Interval::Microsecond(n) => Ok(*timestamp - Duration::microseconds(*n)),
        }
    }

    pub fn add_time(&self, time: &NaiveTime) -> Result<NaiveTime> {
        match self {
            Interval::Month(_) => Err(IntervalError::AddYearOrMonthToTime {
                time: *time,
                interval: *self,
            }
            .into()),
            Interval::Microsecond(n) => Ok(*time + Duration::microseconds(*n)),
        }
    }

    pub fn subtract_from_time(&self, time: &NaiveTime) -> Result<NaiveTime> {
        match self {
            Interval::Month(_) => Err(IntervalError::SubtractYearOrMonthToTime {
                time: *time,
                interval: *self,
            }
            .into()),
            Interval::Microsecond(n) => Ok(*time - Duration::microseconds(*n)),
        }
    }

    pub fn years(years: i32) -> Self {
        Interval::Month(12 * years)
    }

    pub fn months(months: i32) -> Self {
        Interval::Month(months)
    }

    pub fn extract(&self, field: &DateTimeField) -> Result<Value> {
        let value = match (field, *self) {
            (DateTimeField::Year, Interval::Month(i)) => i as i64 / 12,
            (DateTimeField::Month, Interval::Month(i)) => i as i64,
            (DateTimeField::Day, Interval::Microsecond(i)) => i / DAY,
            (DateTimeField::Hour, Interval::Microsecond(i)) => i / HOUR,
            (DateTimeField::Minute, Interval::Microsecond(i)) => i / MINUTE,
            (DateTimeField::Second, Interval::Microsecond(i)) => i / SECOND,
            _ => {
                return Err(IntervalError::FailedToExtract.into());
            }
        };

        Ok(Value::I64(value))
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

    pub fn try_from_str(
        value: &str,
        leading_field: Option<DateTimeField>,
        last_field: Option<DateTimeField>,
    ) -> Result<Self> {
        use DateTimeField::*;

        let value = value.trim_matches('\'');

        let sign = if value.get(0..1) == Some("-") { -1 } else { 1 };

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

        let parse_time = |v: &str| {
            let sign = if v.get(0..1) == Some("-") { -1 } else { 1 };
            let v = v.trim_start_matches('-');
            let time = NaiveTime::from_str(v)
                .map_err(|_| IntervalError::FailedToParseTime(value.to_owned()))?;

            let msec = time.hour() as i64 * HOUR
                + time.minute() as i64 * MINUTE
                + time.second() as i64 * SECOND
                + time.nanosecond() as i64 / 1000;

            Ok(Interval::Microsecond(sign as i64 * msec))
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
                    .trim_start_matches('-')
                    .split('-')
                    .map(parse_integer)
                    .collect::<Result<Vec<_>>>()?;

                match (nums.first(), nums.get(1)) {
                    (Some(years), Some(months)) => {
                        Ok(Interval::months(sign * (12 * years + months)))
                    }
                    _ => Err(IntervalError::FailedToParseYearToMonth(value.to_owned()).into()),
                }
            }
            (Some(Day), Some(Hour)) => {
                let nums = value
                    .trim_start_matches('-')
                    .split(' ')
                    .map(parse_integer)
                    .collect::<Result<Vec<_>>>()?;

                match (nums.first(), nums.get(1)) {
                    (Some(days), Some(hours)) => Ok(Interval::hours(sign * (24 * days + hours))),
                    _ => Err(IntervalError::FailedToParseDayToHour(value.to_owned()).into()),
                }
            }
            (Some(Day), Some(Minute)) => {
                let nums = value.trim_start_matches('-').split(' ').collect::<Vec<_>>();

                match (nums.first(), nums.get(1)) {
                    (Some(days), Some(time)) => {
                        let days = parse_integer(days)?;
                        let time = format!("{}:00", time);

                        Interval::days(days)
                            .add(&parse_time(&time)?)
                            .map(|interval| sign * interval)
                    }
                    _ => Err(IntervalError::FailedToParseDayToMinute(value.to_owned()).into()),
                }
            }
            (Some(Day), Some(Second)) => {
                let nums = value.trim_start_matches('-').split(' ').collect::<Vec<_>>();

                match (nums.first(), nums.get(1)) {
                    (Some(days), Some(time)) => {
                        let days = parse_integer(days)?;

                        Interval::days(days)
                            .add(&parse_time(time)?)
                            .map(|interval| sign * interval)
                    }
                    _ => Err(IntervalError::FailedToParseDayToSecond(value.to_owned()).into()),
                }
            }
            (Some(Hour), Some(Minute)) => parse_time(&format!("{}:00", value)),
            (Some(Hour), Some(Second)) => parse_time(value),
            (Some(Minute), Some(Second)) => {
                let time = value.trim_start_matches('-');

                parse_time(&format!("00:{}", time)).map(|v| sign * v)
            }
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
    use {
        super::{Interval, IntervalError},
        crate::ast::DateTimeField,
        chrono::{NaiveDate, NaiveTime},
    };

    #[test]
    fn cmp() {
        assert!(Interval::Month(12) > Interval::Month(1));
        assert!(Interval::Microsecond(300) > Interval::Microsecond(1));
        assert!(Interval::Month(1) > Interval::Microsecond(1000));
    }

    fn date(year: i32, month: u32, day: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(year, month, day).unwrap()
    }

    fn time(hour: u32, min: u32, sec: u32) -> NaiveTime {
        NaiveTime::from_hms_opt(hour, min, sec).unwrap()
    }

    #[test]
    fn arithmetic() {
        use Interval::*;

        macro_rules! test {
            ($op: ident $a: expr, $b: expr => $c: expr) => {
                assert_eq!($a.$op(&$b), Ok($c));
            };
        }

        assert_eq!(Month(1).unary_minus(), Month(-1));
        assert_eq!(Microsecond(1).unary_minus(), Microsecond(-1));

        // date
        assert_eq!(
            Month(2).add_date(&date(2021, 11, 11)),
            Ok(date(2022, 1, 11).and_hms_opt(0, 0, 0).unwrap())
        );
        assert_eq!(
            Interval::hours(30).add_date(&date(2021, 11, 11)),
            Ok(date(2021, 11, 12).and_hms_opt(6, 0, 0).unwrap())
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
            Ok(date(2021, 9, 11).and_hms_opt(0, 0, 0).unwrap())
        );
        assert_eq!(
            Month(14).subtract_from_date(&date(2021, 11, 11)),
            Ok(date(2020, 9, 11).and_hms_opt(0, 0, 0).unwrap())
        );
        assert_eq!(
            Interval::hours(30).subtract_from_date(&date(2021, 11, 11)),
            Ok(date(2021, 11, 9).and_hms_opt(18, 0, 0).unwrap())
        );
        assert_eq!(
            Interval::years(999_999).subtract_from_date(&date(2021, 11, 11)),
            Err(IntervalError::DateOverflow {
                year: -997977,
                month: -1,
            }
            .into())
        );

        // timestamp
        assert_eq!(
            Interval::minutes(2).add_timestamp(&date(2021, 11, 11).and_hms_opt(12, 3, 1).unwrap()),
            Ok(date(2021, 11, 11).and_hms_opt(12, 5, 1).unwrap())
        );
        assert_eq!(
            Interval::hours(30).add_timestamp(&date(2021, 11, 11).and_hms_opt(0, 30, 0).unwrap()),
            Ok(date(2021, 11, 12).and_hms_opt(6, 30, 0).unwrap())
        );
        assert_eq!(
            Interval::years(999_999)
                .add_timestamp(&date(2021, 11, 11).and_hms_opt(1, 1, 1).unwrap()),
            Err(IntervalError::DateOverflow {
                year: 1_002_020,
                month: 11,
            }
            .into())
        );
        assert_eq!(
            Month(2).subtract_from_timestamp(&date(2021, 11, 11).and_hms_opt(1, 3, 59).unwrap()),
            Ok(date(2021, 9, 11).and_hms_opt(1, 3, 59).unwrap())
        );
        assert_eq!(
            Month(14).subtract_from_timestamp(&date(2021, 11, 11).and_hms_opt(23, 1, 1).unwrap()),
            Ok(date(2020, 9, 11).and_hms_opt(23, 1, 1).unwrap())
        );
        assert_eq!(
            Interval::seconds(30)
                .subtract_from_timestamp(&date(2021, 11, 11).and_hms_opt(0, 0, 0).unwrap()),
            Ok(date(2021, 11, 10).and_hms_opt(23, 59, 30).unwrap())
        );
        assert_eq!(
            Interval::years(999_999)
                .subtract_from_timestamp(&date(2021, 11, 11).and_hms_opt(0, 0, 0).unwrap()),
            Err(IntervalError::DateOverflow {
                year: -997977,
                month: -1,
            }
            .into())
        );

        // time
        assert_eq!(
            Interval::minutes(30).add_time(&time(23, 0, 1)),
            Ok(time(23, 30, 1))
        );
        assert_eq!(
            Interval::hours(20).add_time(&time(5, 30, 0)),
            Ok(time(1, 30, 0))
        );
        assert_eq!(
            Interval::years(1).add_time(&time(23, 0, 1)),
            Err(IntervalError::AddYearOrMonthToTime {
                time: time(23, 0, 1),
                interval: Interval::years(1),
            }
            .into())
        );
        assert_eq!(
            Interval::minutes(30).subtract_from_time(&time(23, 0, 1)),
            Ok(time(22, 30, 1))
        );
        assert_eq!(
            Interval::hours(20).subtract_from_time(&time(5, 30, 0)),
            Ok(time(9, 30, 0))
        );
        assert_eq!(
            Interval::months(3).subtract_from_time(&time(23, 0, 1)),
            Err(IntervalError::SubtractYearOrMonthToTime {
                time: time(23, 0, 1),
                interval: Interval::months(3),
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
                let interval = Interval::try_from_str($value, Some(DateTimeField::$datetime), None);

                assert_eq!(interval, Ok(Interval::$duration($expected_value)));
            };
            ($value: expr, $from: ident to $to: ident => $expected_value: expr, $duration: ident) => {
                let interval = Interval::try_from_str(
                    $value,
                    Some(DateTimeField::$from),
                    Some(DateTimeField::$to),
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
        test!("-10",  Second => -10, seconds);
        test!("10.5", Second => 10_500_000, microseconds);
        test!("-1.5", Second => -1_500_000, microseconds);

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

        test!("-1-4", Year to Month => -16, months);
        test!("-2 10", Day to Hour => -58, hours);
        test!("-1 00:01", Day to Minute => -(24 * 60 + 1), minutes);
        test!("-1 00:00:01", Day to Second => -(24 * 3600 + 1), seconds);
        test!("-1 00:00:01.1", Day to Second => -((24 * 3600 + 1) * 1000 + 100), milliseconds);
        test!("-21:10", Hour to Minute => -(21 * 60 + 10), minutes);
        test!("-05:12:03", Hour to Second => -(5 * 3600 + 12 * 60 + 3), seconds);
        test!("-03:59:22.372", Hour to Second => -((3 * 3600 + 59 * 60 + 22) * 1000 + 372), milliseconds);
        test!("-09:33", Minute to Second => -(9 * 60 + 33), seconds);
        test!("-09:33.192", Minute to Second => -((9 * 60 + 33) * 1000 + 192), milliseconds);
    }
}
