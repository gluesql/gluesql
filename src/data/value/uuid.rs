use {
    super::ValueError,
    crate::result::{Error, Result},
    uuid::Uuid,
};

pub fn parse_uuid(v: &str) -> Result<u128> {
    match Uuid::parse_str(v) {
        Ok(u) => Ok(u.as_u128()),
        _ => Err(Error::Value(ValueError::FailedToParseUUID(v.to_owned()))),
    }
}

#[cfg(test)]
mod tests {
    use crate::data::value::ValueError;

    #[test]
    fn parse_uuid() {
        macro_rules! test (
            ($str: literal, $result: expr) => {
                assert_eq!(super::parse_uuid($str), $result)
            }
        );

        test!(
            "936DA01F9ABD4d9d80C702AF85C822A8",
            Ok(195965723427462096757863453463987888808)
        );
        test!(
            "550e8400-e29b-41d4-a716-446655440000",
            Ok(113059749145936325402354257176981405696)
        );
        test!(
            "urn:uuid:F9168C5E-CEB2-4faa-B6BF-329BF39FA1E4",
            Ok(331094848530093083170738142201201533412)
        );

        test!(
            "1",
            Err(ValueError::FailedToParseUUID("1".to_owned()).into())
        );
        test!(
            "NOT_UUID_STRING",
            Err(ValueError::FailedToParseUUID("NOT_UUID_STRING".to_owned()).into())
        );
    }
}
