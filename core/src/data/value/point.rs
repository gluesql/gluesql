use {
    super::ValueError,
    crate::result::{Error, Result},
};

pub fn parse_point(value: &str) -> Result<(f64, f64)> {
    let v = value.replace("POINT(", "").replace(")", "");
    let mut split = v.split_whitespace();
    let x = split.next();
    let y = split.next();

    match (x, y) {
        (Some(x), Some(y)) => Ok((x.parse::<f64>().unwrap(), y.parse::<f64>().unwrap())),
        (_, _) => Err(Error::Value(ValueError::FailedToParsePoint(
            value.to_owned(),
        ))),
    }
}

#[cfg(test)]
mod tests {
    use crate::data::value::ValueError;

    #[test]
    fn parse_point() {
        macro_rules! test (
            ($str: literal, $result: expr) => {
                assert_eq!(super::parse_point($str), $result)
            }
        );

        test!("POINT(15.0 20.0)", Ok((15.0, 20.0)));
        test!(
            "1",
            Err(ValueError::FailedToParsePoint("1".to_owned()).into())
        );
        test!(
            "NOT_POINT_STRING",
            Err(ValueError::FailedToParsePoint("NOT_POINT_STRING".to_owned()).into())
        );
    }
}
