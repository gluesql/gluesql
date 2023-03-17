use {
    super::Point,
    serde::{Deserialize, Serialize},
    std::collections::HashMap,
};

#[derive(Serialize, Deserialize)]
pub struct Geometry {
    pub r#type: String,
    pub coordinates: Vec<f64>,
}

#[derive(Serialize, Deserialize)]
pub struct Geojson {
    pub r#type: String,
    pub geometry: Geometry,
    pub properties: HashMap<String, String>,
}

impl From<Point> for Geojson {
    fn from(value: Point) -> Geojson {
        let geometry = Geometry {
            r#type: "Point".to_owned(),
            coordinates: vec![value.x, value.y],
        };
        let properties = HashMap::new();
        Geojson {
            r#type: "Feature".to_owned(),
            geometry,
            properties,
        }
    }
}
