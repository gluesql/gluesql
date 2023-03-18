use {
    super::ValueError,
    crate::result::{Error, Result},
    regex::Regex,
    serde::{Deserialize, Serialize},
    std::fmt,
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

    pub fn x(&self) -> f64 {
        self.x
    }

    pub fn y(&self) -> f64 {
        self.y
    }

    pub fn from_wkt(v: &str) -> Result<Self> {
        let re = Regex::new(r"POINT\s*\(\s*(-?\d*\.?\d+)\s+(-?\d*\.?\d+)\s*\)").unwrap();

        if let Some(captures) = re.captures(v) {
            let x = captures[1]
                .parse::<f64>()
                .map_err(|_| Error::Value(ValueError::FailedToParsePoint(v.to_owned())))?;
            let y = captures[2]
                .parse::<f64>()
                .map_err(|_| Error::Value(ValueError::FailedToParsePoint(v.to_owned())))?;
            Ok(Self { x, y })
        } else {
            Err(Error::Value(ValueError::FailedToParsePoint(v.to_owned())))
        }
    }
}

impl PartialEq for Point {
    fn eq(&self, other: &Self) -> bool {
        self.x == other.x && self.y == other.y
    }
}

impl Eq for Point {}

impl fmt::Display for Point {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "POINT({} {})", self.x, self.y)
    }
}
