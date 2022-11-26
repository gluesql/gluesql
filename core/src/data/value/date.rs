use chrono::{offset::Utc, DateTime, NaiveDate, NaiveDateTime, NaiveTime};

pub fn parse_date(v: &str) -> Option<NaiveDate> {
    if let Ok(v) = v.parse::<NaiveDate>() {
        return Some(v);
    }

    let forms = ["%Y-%m-%d", "%m-%d-%Y"];

    let v = v.to_uppercase();

    for form in forms.iter() {
        if let Ok(v) = NaiveDate::parse_from_str(&v, form) {
            return Some(v);
        }
    }

    None
}

pub fn parse_time(v: &str) -> Option<NaiveTime> {
    if let Ok(v) = v.parse::<NaiveTime>() {
        return Some(v);
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
            return Some(v);
        }
    }

    None
}

pub fn parse_timestamp(v: &str) -> Option<NaiveDateTime> {
    if let Ok(v) = v.parse::<DateTime<Utc>>() {
        return Some(v.naive_utc());
    } else if let Ok(v) = v.parse::<NaiveDateTime>() {
        return Some(v);
    } else if let Ok(v) = v.parse::<NaiveDate>() {
        return v.and_hms_opt(0, 0, 0);
    }

    let forms = ["%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M:%S%.f"];

    for form in forms.iter() {
        if let Ok(v) = NaiveDateTime::parse_from_str(v, form) {
            return Some(v);
        }
    }

    None
}
