use {
    super::Value,
    crate::data::Interval,
    chrono::{Datelike, Timelike},
};

const VALUE: u8 = 0;
const NULL: u8 = 1;

impl Value {
    pub fn to_be_bytes(&self) -> Vec<u8> {
        match self {
            Value::Bool(v) => {
                if *v {
                    vec![VALUE, 1]
                } else {
                    vec![VALUE, 0]
                }
            }
            Value::I64(v) => [VALUE]
                .iter()
                .chain(v.to_be_bytes().iter())
                .copied()
                .collect::<Vec<_>>(),
            Value::F64(v) => [VALUE]
                .iter()
                .chain(v.to_be_bytes().iter())
                .copied()
                .collect::<Vec<_>>(),
            Value::Str(v) => [VALUE]
                .iter()
                .chain(v.as_bytes().iter())
                .copied()
                .collect::<Vec<_>>(),
            Value::Date(date) => [VALUE]
                .iter()
                .chain(date.num_days_from_ce().to_be_bytes().iter())
                .copied()
                .collect::<Vec<_>>(),
            Value::Time(time) => {
                let secs = time.num_seconds_from_midnight();
                let frac = time.nanosecond();

                [VALUE]
                    .iter()
                    .chain(secs.to_be_bytes().iter())
                    .chain(frac.to_be_bytes().iter())
                    .copied()
                    .collect::<Vec<_>>()
            }
            Value::Timestamp(datetime) => {
                let date = datetime.num_days_from_ce();
                let secs = datetime.num_seconds_from_midnight();
                let frac = datetime.nanosecond();

                [VALUE]
                    .iter()
                    .chain(date.to_be_bytes().iter())
                    .chain(secs.to_be_bytes().iter())
                    .chain(frac.to_be_bytes().iter())
                    .copied()
                    .collect::<Vec<_>>()
            }
            Value::Interval(interval) => {
                let (month, microsec) = match interval {
                    Interval::Month(month) => (*month, 0),
                    Interval::Microsecond(microsec) => (0, *microsec),
                };

                [VALUE]
                    .iter()
                    .chain(month.to_be_bytes().iter())
                    .chain(microsec.to_be_bytes().iter())
                    .copied()
                    .collect::<Vec<_>>()
            }
            Value::UUID(v) => [VALUE]
                .iter()
                .chain(v.to_be_bytes().iter())
                .copied()
                .collect::<Vec<_>>(),
            Value::Null => vec![NULL],
        }
    }
}

#[cfg(test)]
mod tests {
    use std::cmp::Ordering;

    fn cmp(ls: &[u8], rs: &[u8]) -> Ordering {
        for (l, r) in ls.iter().zip(rs.iter()) {
            match l.cmp(r) {
                Ordering::Equal => continue,
                ordering => return ordering,
            }
        }

        let size_l = ls.len();
        let size_r = rs.len();

        size_l.cmp(&size_r)
    }

    #[test]
    fn cmp_big_edian() {
        use crate::{
            chrono::{NaiveDate, NaiveTime},
            data::{Interval as I, Value::*},
        };

        let null = Null.to_be_bytes();

        let n1 = Bool(true).to_be_bytes();
        let n2 = Bool(false).to_be_bytes();

        assert_eq!(cmp(&n2, &n2), Ordering::Equal);
        assert_eq!(cmp(&n1, &n2), Ordering::Greater);
        assert_eq!(cmp(&n2, &n1), Ordering::Less);
        assert_eq!(cmp(&n1, &null), Ordering::Less);

        let n1 = I64(3).to_be_bytes();
        let n2 = I64(20).to_be_bytes();
        let n3 = I64(100).to_be_bytes();

        assert_eq!(cmp(&n2, &n2), Ordering::Equal);
        assert_eq!(cmp(&n1, &n2), Ordering::Less);
        assert_eq!(cmp(&n3, &n1), Ordering::Greater);
        assert_eq!(cmp(&n1, &null), Ordering::Less);

        let n1 = F64(3.0).to_be_bytes();
        let n2 = F64(100.0).to_be_bytes();
        let n3 = F64(1324.0).to_be_bytes();

        assert_eq!(cmp(&n2, &n2), Ordering::Equal);
        assert_eq!(cmp(&n1, &n2), Ordering::Less);
        assert_eq!(cmp(&n3, &n1), Ordering::Greater);
        assert_eq!(cmp(&n1, &null), Ordering::Less);

        let n1 = Str("a".to_owned()).to_be_bytes();
        let n2 = Str("ab".to_owned()).to_be_bytes();
        let n3 = Str("aaa".to_owned()).to_be_bytes();
        let n4 = Str("aaz".to_owned()).to_be_bytes();
        let n5 = Str("c".to_owned()).to_be_bytes();

        assert_eq!(cmp(&n2, &n2), Ordering::Equal);
        assert_eq!(cmp(&n1, &n2), Ordering::Less);
        assert_eq!(cmp(&n3, &n1), Ordering::Greater);
        assert_eq!(cmp(&n2, &n3), Ordering::Greater);
        assert_eq!(cmp(&n3, &n4), Ordering::Less);
        assert_eq!(cmp(&n5, &n4), Ordering::Greater);
        assert_eq!(cmp(&n1, &null), Ordering::Less);

        let n1 = Date(NaiveDate::from_ymd(2021, 1, 1)).to_be_bytes();
        let n2 = Date(NaiveDate::from_ymd(1989, 3, 20)).to_be_bytes();

        assert_eq!(cmp(&n2, &n2), Ordering::Equal);
        assert_eq!(cmp(&n1, &n2), Ordering::Greater);
        assert_eq!(cmp(&n1, &null), Ordering::Less);

        let n1 = Time(NaiveTime::from_hms_milli(20, 1, 9, 100)).to_be_bytes();
        let n2 = Time(NaiveTime::from_hms_milli(3, 10, 30, 0)).to_be_bytes();

        assert_eq!(cmp(&n2, &n2), Ordering::Equal);
        assert_eq!(cmp(&n1, &n2), Ordering::Greater);
        assert_eq!(cmp(&n1, &null), Ordering::Less);

        let n1 = Timestamp(NaiveDate::from_ymd(2021, 1, 1).and_hms_milli(1, 2, 3, 0)).to_be_bytes();
        let n2 =
            Timestamp(NaiveDate::from_ymd(1989, 3, 20).and_hms_milli(10, 0, 0, 1000)).to_be_bytes();

        assert_eq!(cmp(&n2, &n2), Ordering::Equal);
        assert_eq!(cmp(&n1, &n2), Ordering::Greater);
        assert_eq!(cmp(&n1, &null), Ordering::Less);

        let n1 = Interval(I::Month(30)).to_be_bytes();
        let n2 = Interval(I::Month(2)).to_be_bytes();
        let n3 = Interval(I::Microsecond(1000)).to_be_bytes();
        let n4 = Interval(I::Microsecond(30)).to_be_bytes();

        assert_eq!(cmp(&n1, &n1), Ordering::Equal);
        assert_eq!(cmp(&n2, &n1), Ordering::Less);
        assert_eq!(cmp(&n2, &n3), Ordering::Greater);
        assert_eq!(cmp(&n3, &n4), Ordering::Greater);
        assert_eq!(cmp(&n1, &null), Ordering::Less);

        let n1 = UUID(100).to_be_bytes();
        let n2 = UUID(101).to_be_bytes();

        assert_eq!(cmp(&n1, &n1), Ordering::Equal);
        assert_eq!(cmp(&n1, &n2), Ordering::Less);
        assert_eq!(cmp(&n2, &n1), Ordering::Greater);
        assert_eq!(cmp(&n1, &null), Ordering::Less);
    }
}
