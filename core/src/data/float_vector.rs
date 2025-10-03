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
                write!(f, "Vector dimension {actual} exceeds maximum {max}")
            }
            VectorError::InvalidFloat(msg) => write!(f, "Invalid float value: {msg}"),
            VectorError::DimensionMismatch { expected, actual } => {
                write!(f, "Dimension mismatch: expected {expected}, got {actual}")
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
                return Err(VectorError::InvalidFloat(format!("NaN at index {i}")));
            }
            if f.is_infinite() {
                return Err(VectorError::InvalidFloat(format!(
                    "Infinite value at index {i}"
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
        self.magnitude_squared().sqrt()
    }

    fn magnitude_squared(&self) -> f32 {
        self.data.iter().map(|x| x * x).sum::<f32>()
    }

    pub fn normalize(&self) -> Result<Self, VectorError> {
        let mag = self.magnitude();
        if mag == 0.0 {
            return Err(VectorError::InvalidFloat(
                "Cannot normalize zero vector".to_owned(),
            ));
        }

        let normalized_data: Vec<f32> = self.data.iter().map(|x| x / mag).collect();
        Ok(Self {
            data: normalized_data,
        })
    }

    pub fn dot_product(&self, other: &Self) -> Result<f32, VectorError> {
        if self.dimension() != other.dimension() {
            return Err(VectorError::DimensionMismatch {
                expected: self.dimension(),
                actual: other.dimension(),
            });
        }

        Ok(self
            .data
            .iter()
            .zip(other.data.iter())
            .map(|(a, b)| a * b)
            .sum())
    }

    pub fn add(&self, other: &Self) -> Result<Self, VectorError> {
        if self.dimension() != other.dimension() {
            return Err(VectorError::DimensionMismatch {
                expected: self.dimension(),
                actual: other.dimension(),
            });
        }

        let result_data: Vec<f32> = self
            .data
            .iter()
            .zip(other.data.iter())
            .map(|(a, b)| a + b)
            .collect();
        Ok(Self { data: result_data })
    }

    pub fn subtract(&self, other: &Self) -> Result<Self, VectorError> {
        if self.dimension() != other.dimension() {
            return Err(VectorError::DimensionMismatch {
                expected: self.dimension(),
                actual: other.dimension(),
            });
        }

        let result_data: Vec<f32> = self
            .data
            .iter()
            .zip(other.data.iter())
            .map(|(a, b)| a - b)
            .collect();
        Ok(Self { data: result_data })
    }

    pub fn scalar_multiply(&self, scalar: f32) -> Result<Self, VectorError> {
        if scalar.is_nan() || scalar.is_infinite() {
            return Err(VectorError::InvalidFloat(format!(
                "Invalid scalar: {scalar}"
            )));
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

        let sum_of_squares: f32 = self
            .data
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
            return Err(VectorError::InvalidFloat(
                "Cannot compute cosine similarity with zero vector".to_owned(),
            ));
        }

        Ok(dot / (mag_self * mag_other))
    }

    pub fn manhattan_distance(&self, other: &Self) -> Result<f32, VectorError> {
        if self.dimension() != other.dimension() {
            return Err(VectorError::DimensionMismatch {
                expected: self.dimension(),
                actual: other.dimension(),
            });
        }

        let distance: f32 = self
            .data
            .iter()
            .zip(other.data.iter())
            .map(|(a, b)| (a - b).abs())
            .sum();

        Ok(distance)
    }

    pub fn chebyshev_distance(&self, other: &Self) -> Result<f32, VectorError> {
        if self.dimension() != other.dimension() {
            return Err(VectorError::DimensionMismatch {
                expected: self.dimension(),
                actual: other.dimension(),
            });
        }

        let max_diff = self
            .data
            .iter()
            .zip(other.data.iter())
            .map(|(a, b)| (a - b).abs())
            .fold(0.0f32, |acc, x| acc.max(x));

        Ok(max_diff)
    }

    pub fn hamming_distance(&self, other: &Self) -> Result<u32, VectorError> {
        if self.dimension() != other.dimension() {
            return Err(VectorError::DimensionMismatch {
                expected: self.dimension(),
                actual: other.dimension(),
            });
        }

        let count = self
            .data
            .iter()
            .zip(other.data.iter())
            .map(|(a, b)| if (a - b).abs() > f32::EPSILON { 1 } else { 0 })
            .sum();

        Ok(count)
    }

    pub fn jaccard_similarity(&self, other: &Self) -> Result<f32, VectorError> {
        if self.dimension() != other.dimension() {
            return Err(VectorError::DimensionMismatch {
                expected: self.dimension(),
                actual: other.dimension(),
            });
        }

        let mut intersection = 0.0f32;
        let mut union = 0.0f32;

        for (a, b) in self.data.iter().zip(other.data.iter()) {
            let min_val = a.min(*b);
            let max_val = a.max(*b);

            intersection += min_val;
            union += max_val;
        }

        if union == 0.0 {
            return Ok(1.0); // Both vectors are zero vectors, they're identical
        }

        Ok(intersection / union)
    }

    pub fn minkowski_distance(&self, other: &Self, p: f32) -> Result<f32, VectorError> {
        if self.dimension() != other.dimension() {
            return Err(VectorError::DimensionMismatch {
                expected: self.dimension(),
                actual: other.dimension(),
            });
        }

        if p <= 0.0 || p.is_nan() || p.is_infinite() {
            return Err(VectorError::InvalidFloat(format!(
                "Invalid p parameter for Minkowski distance: {p}"
            )));
        }

        let sum: f32 = self
            .data
            .iter()
            .zip(other.data.iter())
            .map(|(a, b)| (a - b).abs().powf(p))
            .sum();

        Ok(sum.powf(1.0 / p))
    }

    pub fn canberra_distance(&self, other: &Self) -> Result<f32, VectorError> {
        if self.dimension() != other.dimension() {
            return Err(VectorError::DimensionMismatch {
                expected: self.dimension(),
                actual: other.dimension(),
            });
        }

        let distance: f32 = self
            .data
            .iter()
            .zip(other.data.iter())
            .map(|(a, b)| {
                let numerator = (a - b).abs();
                let denominator = a.abs() + b.abs();
                if denominator == 0.0 {
                    0.0 // Both values are zero
                } else {
                    numerator / denominator
                }
            })
            .sum();

        Ok(distance)
    }
}

impl fmt::Display for FloatVector {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "[{}]",
            self.data
                .iter()
                .map(|x| x.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        )
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
        let large_vec: Vec<f32> = (0..FloatVector::MAX_DIMENSIONS + 1)
            .map(|x| x as f32)
            .collect();
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

    #[test]
    fn test_simd_consistency_large_vectors() {
        // Test with vector size that will use SIMD (32 elements = 4 AVX chunks)
        let data1: Vec<f32> = (0..32).map(|i| i as f32 * 0.1).collect();
        let data2: Vec<f32> = (0..32).map(|i| (i + 1) as f32 * 0.2).collect();

        let vec1 = FloatVector::new(data1).unwrap();
        let vec2 = FloatVector::new(data2).unwrap();

        // Test dot product
        let dot_result = vec1.dot_product(&vec2).unwrap();
        let expected_dot: f32 = vec1
            .data
            .iter()
            .zip(vec2.data.iter())
            .map(|(a, b)| a * b)
            .sum();
        assert!((dot_result - expected_dot).abs() < 1e-6);

        // Test magnitude
        let mag = vec1.magnitude();
        let expected_mag = vec1.data.iter().map(|x| x * x).sum::<f32>().sqrt();
        assert!((mag - expected_mag).abs() < 1e-6);

        // Test addition
        let add_result = vec1.add(&vec2).unwrap();
        let expected_add: Vec<f32> = vec1
            .data
            .iter()
            .zip(vec2.data.iter())
            .map(|(a, b)| a + b)
            .collect();
        for (actual, expected) in add_result.data.iter().zip(expected_add.iter()) {
            assert!((actual - expected).abs() < 1e-6);
        }

        // Test subtraction
        let sub_result = vec1.subtract(&vec2).unwrap();
        let expected_sub: Vec<f32> = vec1
            .data
            .iter()
            .zip(vec2.data.iter())
            .map(|(a, b)| a - b)
            .collect();
        for (actual, expected) in sub_result.data.iter().zip(expected_sub.iter()) {
            assert!((actual - expected).abs() < 1e-6);
        }
    }

    #[test]
    fn test_simd_consistency_odd_sizes() {
        // Test with vector sizes that don't align perfectly with SIMD (33 elements)
        let data1: Vec<f32> = (0..33).map(|i| i as f32 * 0.1).collect();
        let data2: Vec<f32> = (0..33).map(|i| (i + 1) as f32 * 0.2).collect();

        let vec1 = FloatVector::new(data1).unwrap();
        let vec2 = FloatVector::new(data2).unwrap();

        // Test that results are consistent
        let dot_result = vec1.dot_product(&vec2).unwrap();
        let expected_dot: f32 = vec1
            .data
            .iter()
            .zip(vec2.data.iter())
            .map(|(a, b)| a * b)
            .sum();
        assert!((dot_result - expected_dot).abs() < 1e-6);
    }

    #[test]
    fn test_from_slice() {
        let data = [1.0, 2.0, 3.0];
        let vec = FloatVector::from_slice(&data).unwrap();
        assert_eq!(vec.dimension(), 3);
        assert_eq!(vec.data(), &[1.0, 2.0, 3.0]);

        // Test with empty slice
        let empty_data: &[f32] = &[];
        let result = FloatVector::from_slice(empty_data);
        assert!(matches!(result, Err(VectorError::EmptyVector)));
    }

    #[test]
    fn test_get_method() {
        let vec = FloatVector::new(vec![1.0, 2.0, 3.0]).unwrap();
        
        assert_eq!(vec.get(0), Some(1.0));
        assert_eq!(vec.get(1), Some(2.0));
        assert_eq!(vec.get(2), Some(3.0));
        assert_eq!(vec.get(3), None); // Out of bounds
        assert_eq!(vec.get(999), None); // Way out of bounds
    }

    #[test]
    fn test_normalize() {
        // Test normal case
        let vec = FloatVector::new(vec![3.0, 4.0]).unwrap();
        let normalized = vec.normalize().unwrap();
        assert!((normalized.magnitude() - 1.0).abs() < 1e-6);
        assert_eq!(normalized.data(), &[0.6, 0.8]);

        // Test zero vector (should fail)
        let zero_vec = FloatVector::new(vec![0.0, 0.0]).unwrap();
        let result = zero_vec.normalize();
        assert!(matches!(result, Err(VectorError::InvalidFloat(_))));

        // Test single element vector
        let single_vec = FloatVector::new(vec![5.0]).unwrap();
        let normalized_single = single_vec.normalize().unwrap();
        assert!((normalized_single.magnitude() - 1.0).abs() < 1e-6);
        assert_eq!(normalized_single.data(), &[1.0]);
    }

    #[test]
    fn test_subtract() {
        let vec1 = FloatVector::new(vec![5.0, 3.0, 8.0]).unwrap();
        let vec2 = FloatVector::new(vec![2.0, 1.0, 3.0]).unwrap();
        let result = vec1.subtract(&vec2).unwrap();
        assert_eq!(result.data(), &[3.0, 2.0, 5.0]);

        // Test dimension mismatch
        let vec3 = FloatVector::new(vec![1.0, 2.0]).unwrap();
        let result = vec1.subtract(&vec3);
        assert!(matches!(result, Err(VectorError::DimensionMismatch { .. })));
    }

    #[test]
    fn test_scalar_multiply() {
        let vec = FloatVector::new(vec![1.0, 2.0, 3.0]).unwrap();
        
        // Test positive scalar
        let result = vec.scalar_multiply(2.0).unwrap();
        assert_eq!(result.data(), &[2.0, 4.0, 6.0]);

        // Test negative scalar
        let result = vec.scalar_multiply(-1.5).unwrap();
        assert_eq!(result.data(), &[-1.5, -3.0, -4.5]);

        // Test zero scalar
        let result = vec.scalar_multiply(0.0).unwrap();
        assert_eq!(result.data(), &[0.0, 0.0, 0.0]);

        // Test invalid scalar - NaN
        let result = vec.scalar_multiply(f32::NAN);
        assert!(matches!(result, Err(VectorError::InvalidFloat(_))));

        // Test invalid scalar - Infinity
        let result = vec.scalar_multiply(f32::INFINITY);
        assert!(matches!(result, Err(VectorError::InvalidFloat(_))));
    }

    #[test]
    fn test_euclidean_distance() {
        let vec1 = FloatVector::new(vec![0.0, 0.0]).unwrap();
        let vec2 = FloatVector::new(vec![3.0, 4.0]).unwrap();
        let distance = vec1.euclidean_distance(&vec2).unwrap();
        assert_eq!(distance, 5.0); // sqrt(3^2 + 4^2) = 5

        // Test identical vectors
        let distance = vec1.euclidean_distance(&vec1).unwrap();
        assert_eq!(distance, 0.0);

        // Test dimension mismatch
        let vec3 = FloatVector::new(vec![1.0, 2.0, 3.0]).unwrap();
        let result = vec1.euclidean_distance(&vec3);
        assert!(matches!(result, Err(VectorError::DimensionMismatch { .. })));
    }

    #[test]
    fn test_cosine_similarity() {
        // Test identical vectors
        let vec1 = FloatVector::new(vec![1.0, 2.0, 3.0]).unwrap();
        let similarity = vec1.cosine_similarity(&vec1).unwrap();
        assert!((similarity - 1.0).abs() < 1e-6);

        // Test orthogonal vectors
        let vec2 = FloatVector::new(vec![1.0, 0.0]).unwrap();
        let vec3 = FloatVector::new(vec![0.0, 1.0]).unwrap();
        let similarity = vec2.cosine_similarity(&vec3).unwrap();
        assert!((similarity - 0.0).abs() < 1e-6);

        // Test zero vector (should fail)
        let zero_vec = FloatVector::new(vec![0.0, 0.0]).unwrap();
        let result = vec2.cosine_similarity(&zero_vec);
        assert!(matches!(result, Err(VectorError::InvalidFloat(_))));

        // Test dimension mismatch
        let vec4 = FloatVector::new(vec![1.0, 2.0, 3.0]).unwrap();
        let result = vec2.cosine_similarity(&vec4);
        assert!(matches!(result, Err(VectorError::DimensionMismatch { .. })));
    }

    #[test]
    fn test_manhattan_distance() {
        let vec1 = FloatVector::new(vec![1.0, 2.0, 3.0]).unwrap();
        let vec2 = FloatVector::new(vec![4.0, 6.0, 8.0]).unwrap();
        let distance = vec1.manhattan_distance(&vec2).unwrap();
        assert_eq!(distance, 12.0); // |1-4| + |2-6| + |3-8| = 3 + 4 + 5 = 12

        // Test identical vectors
        let distance = vec1.manhattan_distance(&vec1).unwrap();
        assert_eq!(distance, 0.0);

        // Test dimension mismatch
        let vec3 = FloatVector::new(vec![1.0, 2.0]).unwrap();
        let result = vec1.manhattan_distance(&vec3);
        assert!(matches!(result, Err(VectorError::DimensionMismatch { .. })));
    }

    #[test]
    fn test_chebyshev_distance() {
        let vec1 = FloatVector::new(vec![1.0, 2.0, 3.0]).unwrap();
        let vec2 = FloatVector::new(vec![4.0, 5.0, 6.0]).unwrap();
        let distance = vec1.chebyshev_distance(&vec2).unwrap();
        assert_eq!(distance, 3.0); // max(|1-4|, |2-5|, |3-6|) = max(3, 3, 3) = 3

        // Test with different max values
        let vec3 = FloatVector::new(vec![1.0, 10.0, 3.0]).unwrap();
        let distance = vec1.chebyshev_distance(&vec3).unwrap();
        assert_eq!(distance, 8.0); // max(|1-1|, |2-10|, |3-3|) = max(0, 8, 0) = 8

        // Test dimension mismatch
        let vec4 = FloatVector::new(vec![1.0, 2.0]).unwrap();
        let result = vec1.chebyshev_distance(&vec4);
        assert!(matches!(result, Err(VectorError::DimensionMismatch { .. })));
    }

    #[test]
    fn test_hamming_distance() {
        // Test binary-like vectors
        let vec1 = FloatVector::new(vec![1.0, 0.0, 1.0, 0.0]).unwrap();
        let vec2 = FloatVector::new(vec![1.0, 1.0, 0.0, 0.0]).unwrap();
        let distance = vec1.hamming_distance(&vec2).unwrap();
        assert_eq!(distance, 2); // positions 1 and 2 differ

        // Test identical vectors
        let distance = vec1.hamming_distance(&vec1).unwrap();
        assert_eq!(distance, 0);

        // Test with floating point precision
        let vec3 = FloatVector::new(vec![1.0, 0.001]).unwrap(); // Larger than EPSILON
        let vec4 = FloatVector::new(vec![1.0, 0.0]).unwrap();
        let distance = vec3.hamming_distance(&vec4).unwrap();
        assert_eq!(distance, 1); // Should count as different due to epsilon threshold

        // Test dimension mismatch
        let vec5 = FloatVector::new(vec![1.0, 2.0, 3.0]).unwrap();
        let result = vec1.hamming_distance(&vec5);
        assert!(matches!(result, Err(VectorError::DimensionMismatch { .. })));
    }

    #[test]
    fn test_jaccard_similarity() {
        // Test typical case
        let vec1 = FloatVector::new(vec![1.0, 0.0, 1.0]).unwrap();
        let vec2 = FloatVector::new(vec![1.0, 1.0, 0.0]).unwrap();
        let similarity = vec1.jaccard_similarity(&vec2).unwrap();
        // |A ∩ B| = min(1,1) + min(0,1) + min(1,0) = 1 + 0 + 0 = 1
        // |A ∪ B| = max(1,1) + max(0,1) + max(1,0) = 1 + 1 + 1 = 3
        // Jaccard = 1/3 ≈ 0.333
        assert!((similarity - 0.33333334).abs() < 1e-6);

        // Test identical vectors
        let similarity = vec1.jaccard_similarity(&vec1).unwrap();
        assert!((similarity - 1.0).abs() < 1e-6);

        // Test both zero vectors (edge case)
        let zero_vec1 = FloatVector::new(vec![0.0, 0.0]).unwrap();
        let zero_vec2 = FloatVector::new(vec![0.0, 0.0]).unwrap();
        let similarity = zero_vec1.jaccard_similarity(&zero_vec2).unwrap();
        assert_eq!(similarity, 1.0); // Both are identical zero vectors

        // Test dimension mismatch
        let vec3 = FloatVector::new(vec![1.0, 2.0]).unwrap();
        let result = vec1.jaccard_similarity(&vec3);
        assert!(matches!(result, Err(VectorError::DimensionMismatch { .. })));
    }

    #[test]
    fn test_minkowski_distance() {
        let vec1 = FloatVector::new(vec![1.0, 2.0]).unwrap();
        let vec2 = FloatVector::new(vec![4.0, 6.0]).unwrap();
        
        // Test p = 1 (Manhattan distance)
        let distance = vec1.minkowski_distance(&vec2, 1.0).unwrap();
        assert_eq!(distance, 7.0); // |1-4| + |2-6| = 3 + 4 = 7

        // Test p = 2 (Euclidean distance)
        let distance = vec1.minkowski_distance(&vec2, 2.0).unwrap();
        assert_eq!(distance, 5.0); // sqrt(3^2 + 4^2) = sqrt(25) = 5

        // Test p = 3
        let distance = vec1.minkowski_distance(&vec2, 3.0).unwrap();
        let expected = (27.0_f32 + 64.0_f32).powf(1.0/3.0); // (3^3 + 4^3)^(1/3)
        assert!((distance - expected).abs() < 1e-6);

        // Test invalid p values
        let result = vec1.minkowski_distance(&vec2, 0.0);
        assert!(matches!(result, Err(VectorError::InvalidFloat(_))));

        let result = vec1.minkowski_distance(&vec2, -1.0);
        assert!(matches!(result, Err(VectorError::InvalidFloat(_))));

        let result = vec1.minkowski_distance(&vec2, f32::NAN);
        assert!(matches!(result, Err(VectorError::InvalidFloat(_))));

        let result = vec1.minkowski_distance(&vec2, f32::INFINITY);
        assert!(matches!(result, Err(VectorError::InvalidFloat(_))));

        // Test dimension mismatch
        let vec3 = FloatVector::new(vec![1.0, 2.0, 3.0]).unwrap();
        let result = vec1.minkowski_distance(&vec3, 2.0);
        assert!(matches!(result, Err(VectorError::DimensionMismatch { .. })));
    }

    #[test]
    fn test_canberra_distance() {
        let vec1 = FloatVector::new(vec![1.0, 2.0]).unwrap();
        let vec2 = FloatVector::new(vec![3.0, 4.0]).unwrap();
        let distance = vec1.canberra_distance(&vec2).unwrap();
        // |1-3|/(|1|+|3|) + |2-4|/(|2|+|4|) = 2/4 + 2/6 = 0.5 + 0.333... ≈ 0.8333
        let expected = 2.0/4.0 + 2.0/6.0;
        assert!((distance - expected).abs() < 1e-6);

        // Test with zero denominators (both values are zero)
        let vec3 = FloatVector::new(vec![0.0, 2.0]).unwrap();
        let vec4 = FloatVector::new(vec![0.0, 4.0]).unwrap();
        let distance = vec3.canberra_distance(&vec4).unwrap();
        // 0/(0+0) + |2-4|/(2+4) = 0 + 2/6 = 0.333...
        let expected = 0.0 + 2.0/6.0;
        assert!((distance - expected).abs() < 1e-6);

        // Test identical vectors
        let distance = vec1.canberra_distance(&vec1).unwrap();
        assert_eq!(distance, 0.0);

        // Test dimension mismatch
        let vec5 = FloatVector::new(vec![1.0, 2.0, 3.0]).unwrap();
        let result = vec1.canberra_distance(&vec5);
        assert!(matches!(result, Err(VectorError::DimensionMismatch { .. })));
    }

    #[test]
    fn test_magnitude_squared() {
        let vec = FloatVector::new(vec![3.0, 4.0]).unwrap();
        let mag_squared = vec.magnitude_squared();
        assert_eq!(mag_squared, 25.0); // 3^2 + 4^2 = 9 + 16 = 25
        
        // Test with zero vector
        let zero_vec = FloatVector::new(vec![0.0, 0.0]).unwrap();
        let mag_squared = zero_vec.magnitude_squared();
        assert_eq!(mag_squared, 0.0);
    }

    #[test]
    fn test_edge_cases_with_extreme_values() {
        // Test with very small values
        let small_vec1 = FloatVector::new(vec![1e-10, 2e-10]).unwrap();
        let small_vec2 = FloatVector::new(vec![3e-10, 4e-10]).unwrap();
        let distance = small_vec1.euclidean_distance(&small_vec2).unwrap();
        assert!(distance > 0.0);

        // Test with very large values (but not infinite)
        let large_vec1 = FloatVector::new(vec![1e6, 2e6]).unwrap();
        let large_vec2 = FloatVector::new(vec![3e6, 4e6]).unwrap();
        let distance = large_vec1.euclidean_distance(&large_vec2).unwrap();
        assert!(distance > 0.0);
        assert!(distance.is_finite());
    }
}
