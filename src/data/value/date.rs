use {super::error::ValueError, crate::result::Result, chrono::NaiveDate};

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
