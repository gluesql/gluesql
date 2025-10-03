use {
    serde::{Deserialize, Serialize},
    std::fmt,
};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FloatVector {
    data: Vec<f32>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum VectorError {
    TooManyDimensions { max: usize, actual: usize },
    InvalidFloat(String),
    DimensionMismatch { expected: usize, actual: usize },
    EmptyVector,
}

impl fmt::Display for VectorError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            VectorError::TooManyDimensions { max, actual } => {
                write!(f, "Vector dimension {} exceeds maximum {}", actual, max)
            }
            VectorError::InvalidFloat(msg) => write!(f, "Invalid float value: {}", msg),
            VectorError::DimensionMismatch { expected, actual } => {
                write!(f, "Dimension mismatch: expected {}, got {}", expected, actual)
            }
            VectorError::EmptyVector => write!(f, "Vector cannot be empty"),
        }
    }
}

impl std::error::Error for VectorError {}

impl FloatVector {
    pub const MAX_DIMENSIONS: usize = 1024;

    pub fn new(data: Vec<f32>) -> Result<Self, VectorError> {
        if data.is_empty() {
            return Err(VectorError::EmptyVector);
        }

        if data.len() > Self::MAX_DIMENSIONS {
            return Err(VectorError::TooManyDimensions {
                max: Self::MAX_DIMENSIONS,
                actual: data.len(),
            });
        }

        for (i, &f) in data.iter().enumerate() {
            if f.is_nan() {
                return Err(VectorError::InvalidFloat(format!("NaN at index {}", i)));
            }
            if f.is_infinite() {
                return Err(VectorError::InvalidFloat(format!(
                    "Infinite value at index {}",
                    i
                )));
            }
        }

        Ok(Self { data })
    }

    pub fn from_slice(slice: &[f32]) -> Result<Self, VectorError> {
        Self::new(slice.to_vec())
    }

    pub fn dimension(&self) -> usize {
        self.data.len()
    }

    pub fn data(&self) -> &[f32] {
        &self.data
    }

    pub fn get(&self, index: usize) -> Option<f32> {
        self.data.get(index).copied()
    }

    pub fn magnitude(&self) -> f32 {
        self.data.iter().map(|x| x * x).sum::<f32>().sqrt()
    }

    pub fn normalize(&self) -> Result<Self, VectorError> {
        let mag = self.magnitude();
        if mag == 0.0 {
            return Err(VectorError::InvalidFloat("Cannot normalize zero vector".to_string()));
        }
        
        let normalized_data: Vec<f32> = self.data.iter().map(|x| x / mag).collect();
        Ok(Self { data: normalized_data })
    }

    pub fn dot_product(&self, other: &Self) -> Result<f32, VectorError> {
        if self.dimension() != other.dimension() {
            return Err(VectorError::DimensionMismatch {
                expected: self.dimension(),
                actual: other.dimension(),
            });
        }

        Ok(self.data.iter().zip(other.data.iter()).map(|(a, b)| a * b).sum())
    }

    pub fn add(&self, other: &Self) -> Result<Self, VectorError> {
        if self.dimension() != other.dimension() {
            return Err(VectorError::DimensionMismatch {
                expected: self.dimension(),
                actual: other.dimension(),
            });
        }

        let result_data: Vec<f32> = self.data.iter().zip(other.data.iter()).map(|(a, b)| a + b).collect();
        Ok(Self { data: result_data })
    }

    pub fn subtract(&self, other: &Self) -> Result<Self, VectorError> {
        if self.dimension() != other.dimension() {
            return Err(VectorError::DimensionMismatch {
                expected: self.dimension(),
                actual: other.dimension(),
            });
        }

        let result_data: Vec<f32> = self.data.iter().zip(other.data.iter()).map(|(a, b)| a - b).collect();
        Ok(Self { data: result_data })
    }

    pub fn scalar_multiply(&self, scalar: f32) -> Result<Self, VectorError> {
        if scalar.is_nan() || scalar.is_infinite() {
            return Err(VectorError::InvalidFloat(format!("Invalid scalar: {}", scalar)));
        }

        let result_data: Vec<f32> = self.data.iter().map(|x| x * scalar).collect();
        Ok(Self { data: result_data })
    }

    pub fn euclidean_distance(&self, other: &Self) -> Result<f32, VectorError> {
        if self.dimension() != other.dimension() {
            return Err(VectorError::DimensionMismatch {
                expected: self.dimension(),
                actual: other.dimension(),
            });
        }

        let sum_of_squares: f32 = self.data
            .iter()
            .zip(other.data.iter())
            .map(|(a, b)| (a - b).powi(2))
            .sum();
        
        Ok(sum_of_squares.sqrt())
    }

    pub fn cosine_similarity(&self, other: &Self) -> Result<f32, VectorError> {
        let dot = self.dot_product(other)?;
        let mag_self = self.magnitude();
        let mag_other = other.magnitude();
        
        if mag_self == 0.0 || mag_other == 0.0 {
            return Err(VectorError::InvalidFloat("Cannot compute cosine similarity with zero vector".to_string()));
        }
        
        Ok(dot / (mag_self * mag_other))
    }
}

impl fmt::Display for FloatVector {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[{}]", 
               self.data.iter()
                   .map(|x| x.to_string())
                   .collect::<Vec<_>>()
                   .join(", "))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_vector() {
        let vec = FloatVector::new(vec![1.0, 2.0, 3.0]).unwrap();
        assert_eq!(vec.dimension(), 3);
        assert_eq!(vec.data(), &[1.0, 2.0, 3.0]);
    }

    #[test]
    fn test_empty_vector() {
        let result = FloatVector::new(vec![]);
        assert!(matches!(result, Err(VectorError::EmptyVector)));
    }

    #[test]
    fn test_invalid_float() {
        let result = FloatVector::new(vec![1.0, f32::NAN, 3.0]);
        assert!(matches!(result, Err(VectorError::InvalidFloat(_))));
        
        let result = FloatVector::new(vec![1.0, f32::INFINITY, 3.0]);
        assert!(matches!(result, Err(VectorError::InvalidFloat(_))));
    }

    #[test]
    fn test_too_many_dimensions() {
        let large_vec: Vec<f32> = (0..FloatVector::MAX_DIMENSIONS + 1).map(|x| x as f32).collect();
        let result = FloatVector::new(large_vec);
        assert!(matches!(result, Err(VectorError::TooManyDimensions { .. })));
    }

    #[test]
    fn test_magnitude() {
        let vec = FloatVector::new(vec![3.0, 4.0]).unwrap();
        assert_eq!(vec.magnitude(), 5.0);
    }

    #[test]
    fn test_dot_product() {
        let vec1 = FloatVector::new(vec![1.0, 2.0, 3.0]).unwrap();
        let vec2 = FloatVector::new(vec![4.0, 5.0, 6.0]).unwrap();
        let result = vec1.dot_product(&vec2).unwrap();
        assert_eq!(result, 32.0); // 1*4 + 2*5 + 3*6 = 32
    }

    #[test]
    fn test_vector_addition() {
        let vec1 = FloatVector::new(vec![1.0, 2.0]).unwrap();
        let vec2 = FloatVector::new(vec![3.0, 4.0]).unwrap();
        let result = vec1.add(&vec2).unwrap();
        assert_eq!(result.data(), &[4.0, 6.0]);
    }

    #[test]
    fn test_dimension_mismatch() {
        let vec1 = FloatVector::new(vec![1.0, 2.0]).unwrap();
        let vec2 = FloatVector::new(vec![3.0, 4.0, 5.0]).unwrap();
        let result = vec1.dot_product(&vec2);
        assert!(matches!(result, Err(VectorError::DimensionMismatch { .. })));
    }
}