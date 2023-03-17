use super::geojson::{Geojson, Geometry};

use {
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

impl From<Geojson> for Point {
    fn from(value: Geojson) -> Point {
        Point::new(value.geometry.coordinates[0], value.geometry.coordinates[1])
    }
}

impl From<Geometry> for Point {
    fn from(value: Geometry) -> Point {
        Point::new(value.coordinates[0], value.coordinates[1])
    }
}
