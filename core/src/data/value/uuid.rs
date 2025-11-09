use {super::ValueError, crate::result::Result, uuid::Uuid};

pub fn parse_uuid(v: &str) -> Result<u128> {
    match Uuid::parse_str(v) {
        Ok(u) => Ok(u.as_u128()),
        _ => Err(ValueError::FailedToParseUUID(v.to_owned()).into()),
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
            Ok(195_965_723_427_462_096_757_863_453_463_987_888_808)
        );
        test!(
            "550e8400-e29b-41d4-a716-446655440000",
            Ok(113_059_749_145_936_325_402_354_257_176_981_405_696)
        );
        test!(
            "urn:uuid:F9168C5E-CEB2-4faa-B6BF-329BF39FA1E4",
            Ok(331_094_848_530_093_083_170_738_142_201_201_533_412)
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
