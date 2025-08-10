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
        let x_eq = (self.x.is_nan() && other.x.is_nan())
            || (self.x == 0.0 && other.x == 0.0)
            || self.x == other.x;
        let y_eq = (self.y.is_nan() && other.y.is_nan())
            || (self.y == 0.0 && other.y == 0.0)
            || self.y == other.y;
        x_eq && y_eq
    }
}

impl Eq for Point {}

impl Hash for Point {
    fn hash<H: Hasher>(&self, state: &mut H) {
        const CANONICAL_NAN: u64 = 0x7ff8000000000000;
        const CANONICAL_ZERO: u64 = 0x0000000000000000;

        let x_bits = if self.x.is_nan() {
            CANONICAL_NAN
        } else if self.x == 0.0 {
            CANONICAL_ZERO
        } else {
            self.x.to_bits()
        };

        let y_bits = if self.y.is_nan() {
            CANONICAL_NAN
        } else if self.y == 0.0 {
            CANONICAL_ZERO
        } else {
            self.y.to_bits()
        };

        x_bits.hash(state);
        y_bits.hash(state);
    }
}

impl fmt::Display for Point {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "POINT({} {})", self.x, self.y)
    }
}
