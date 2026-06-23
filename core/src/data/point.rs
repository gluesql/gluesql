use {
    super::ValueError,
    crate::result::Result,
    regex::Regex,
    serde::{Deserialize, Serialize},
    std::{
        fmt,
        hash::{Hash, Hasher},
    },
};

#[derive(Copy, Debug, Clone, Serialize, Deserialize)]
pub struct Point {
    pub x: f64,
    pub y: f64,
}

impl Point {
    pub fn new(x: f64, y: f64) -> Self {
        Self { x, y }
    }

    pub fn from_wkt(v: &str) -> Result<Self> {
        let re = Regex::new(r"POINT\s*\(\s*(-?\d*\.?\d+)\s+(-?\d*\.?\d+)\s*\)").unwrap();

        if let Some(captures) = re.captures(v) {
            let x = captures[1]
                .parse::<f64>()
                .map_err(|_| ValueError::FailedToParsePoint(v.to_owned()))?;
            let y = captures[2]
                .parse::<f64>()
                .map_err(|_| ValueError::FailedToParsePoint(v.to_owned()))?;
            Ok(Self { x, y })
        } else {
            Err(ValueError::FailedToParsePoint(v.to_owned()).into())
        }
    }

    pub fn calc_distance(&self, other: &Point) -> f64 {
        let dx = self.x - other.x;
        let dy = self.y - other.y;
        f64::sqrt(dx * dx + dy * dy)
    }
}

impl PartialEq for Point {
    fn eq(&self, other: &Self) -> bool {
        let points_equal = |a: f64, b: f64| (a.is_nan() && b.is_nan()) || a == b;

        points_equal(self.x, other.x) && points_equal(self.y, other.y)
    }
}

impl Eq for Point {}

impl Hash for Point {
    fn hash<H: Hasher>(&self, state: &mut H) {
        const CANONICAL_F64_NAN_BITS: u64 = 0x7ff8_0000_0000_0000;
        const CANONICAL_F64_ZERO_BITS: u64 = 0;

        #[inline]
        fn normalize_nan_and_zero(x: f64) -> u64 {
            if x.is_nan() {
                CANONICAL_F64_NAN_BITS
            } else if x == 0.0 {
                CANONICAL_F64_ZERO_BITS
            } else {
                x.to_bits()
            }
        }

        let x_bits = normalize_nan_and_zero(self.x);
        let y_bits = normalize_nan_and_zero(self.y);

        x_bits.hash(state);
        y_bits.hash(state);
    }
}

impl fmt::Display for Point {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "POINT({} {})", self.x, self.y)
    }
}
