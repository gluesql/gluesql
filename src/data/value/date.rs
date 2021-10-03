use {
    super::error::ValueError,
    crate::result::Result,
    chrono::{offset::Utc, DateTime, NaiveDate, NaiveDateTime, NaiveTime},
};

pub fn parse_date(v: &str) -> Result<NaiveDate> {
    if let Ok(v) = v.parse::<NaiveDate>() {
        return Ok(v);
    }

    let forms = ["%Y-%m-%d", "%m-%d-%Y"];

    let v = v.to_uppercase();

    for form in forms.iter() {
        if let Ok(v) = NaiveDate::parse_from_str(&v, form) {
            return Ok(v);
        }
    }

    Err(ValueError::FailedToParseDate(v).into())
}

pub fn parse_time(v: &str) -> Result<NaiveTime> {
    if let Ok(v) = v.parse::<NaiveTime>() {
        return Ok(v);
    }

    let forms = [
        "%P %I:%M",
        "%P %l:%M",
        "%P %I:%M:%S",
        "%P %l:%M:%S",
        "%P %I:%M:%S%.f",
        "%P %l:%M:%S%.f",
        "%I:%M %P",
        "%l:%M %P",
        "%I:%M:%S %P",
        "%l:%M:%S %P",
        "%I:%M:%S%.f %P",
        "%l:%M:%S%.f %P",
    ];

    let v = v.to_uppercase();

    for form in forms.iter() {
        if let Ok(v) = NaiveTime::parse_from_str(&v, form) {
            return Ok(v);
        }
    }

    Err(ValueError::FailedToParseTime(v).into())
}

pub fn parse_timestamp(v: &str) -> Result<NaiveDateTime> {
    if let Ok(v) = v.parse::<DateTime<Utc>>() {
        return Ok(v.naive_utc());
    } else if let Ok(v) = v.parse::<NaiveDateTime>() {
        return Ok(v);
    } else if let Ok(v) = v.parse::<NaiveDate>() {
        return Ok(v.and_hms(0, 0, 0));
    }

    let forms = ["%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M:%S%.f"];

    for form in forms.iter() {
        if let Ok(v) = NaiveDateTime::parse_from_str(v, form) {
            return Ok(v);
        }
    }

    Err(ValueError::FailedToParseTimestamp(v.to_string()).into())
}
