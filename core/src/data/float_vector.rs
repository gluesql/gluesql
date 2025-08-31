use {
    serde::{Deserialize, Serialize},
    std::fmt,
};

#[cfg(target_arch = "x86_64")]
use std::arch::x86_64::*;

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
        self.magnitude_squared_simd().sqrt()
    }

    #[cfg(target_arch = "x86_64")]
    fn magnitude_squared_simd(&self) -> f32 {
        if is_x86_feature_detected!("avx") {
            self.magnitude_squared_avx()
        } else if is_x86_feature_detected!("sse") {
            self.magnitude_squared_sse()
        } else {
            self.magnitude_squared_scalar()
        }
    }

    #[cfg(not(target_arch = "x86_64"))]
    fn magnitude_squared_simd(&self) -> f32 {
        self.magnitude_squared_scalar()
    }

    fn magnitude_squared_scalar(&self) -> f32 {
        self.data.iter().map(|x| x * x).sum::<f32>()
    }

    #[cfg(target_arch = "x86_64")]
    #[target_feature(enable = "avx")]
    unsafe fn magnitude_squared_avx(&self) -> f32 {
        let mut sum = _mm256_setzero_ps();
        let chunks = self.data.len() / 8;
        
        for i in 0..chunks {
            let offset = i * 8;
            let a = _mm256_loadu_ps(self.data.as_ptr().add(offset));
            let squared = _mm256_mul_ps(a, a);
            sum = _mm256_add_ps(sum, squared);
        }
        
        let mut result = [0.0f32; 8];
        _mm256_storeu_ps(result.as_mut_ptr(), sum);
        let mut total = result.iter().sum::<f32>();
        
        // Handle remaining elements
        for i in (chunks * 8)..self.data.len() {
            total += self.data[i] * self.data[i];
        }
        
        total
    }

    #[cfg(target_arch = "x86_64")]
    #[target_feature(enable = "sse")]
    unsafe fn magnitude_squared_sse(&self) -> f32 {
        let mut sum = _mm_setzero_ps();
        let chunks = self.data.len() / 4;
        
        for i in 0..chunks {
            let offset = i * 4;
            let a = _mm_loadu_ps(self.data.as_ptr().add(offset));
            let squared = _mm_mul_ps(a, a);
            sum = _mm_add_ps(sum, squared);
        }
        
        let mut result = [0.0f32; 4];
        _mm_storeu_ps(result.as_mut_ptr(), sum);
        let mut total = result.iter().sum::<f32>();
        
        // Handle remaining elements
        for i in (chunks * 4)..self.data.len() {
            total += self.data[i] * self.data[i];
        }
        
        total
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

        Ok(self.dot_product_simd(other))
    }

    #[cfg(target_arch = "x86_64")]
    fn dot_product_simd(&self, other: &Self) -> f32 {
        // Check if AVX is available
        if is_x86_feature_detected!("avx") {
            self.dot_product_avx(other)
        } else if is_x86_feature_detected!("sse") {
            self.dot_product_sse(other)
        } else {
            self.dot_product_scalar(other)
        }
    }

    #[cfg(not(target_arch = "x86_64"))]
    fn dot_product_simd(&self, other: &Self) -> f32 {
        self.dot_product_scalar(other)
    }

    fn dot_product_scalar(&self, other: &Self) -> f32 {
        self.data.iter().zip(other.data.iter()).map(|(a, b)| a * b).sum()
    }

    #[cfg(target_arch = "x86_64")]
    #[target_feature(enable = "avx")]
    unsafe fn dot_product_avx(&self, other: &Self) -> f32 {
        let mut sum = _mm256_setzero_ps();
        let chunks = self.data.len() / 8;
        
        for i in 0..chunks {
            let offset = i * 8;
            let a = _mm256_loadu_ps(self.data.as_ptr().add(offset));
            let b = _mm256_loadu_ps(other.data.as_ptr().add(offset));
            let mul = _mm256_mul_ps(a, b);
            sum = _mm256_add_ps(sum, mul);
        }
        
        // Horizontal sum of the 8 floats in the AVX register
        let mut result = [0.0f32; 8];
        _mm256_storeu_ps(result.as_mut_ptr(), sum);
        let mut total = result.iter().sum::<f32>();
        
        // Handle remaining elements
        for i in (chunks * 8)..self.data.len() {
            total += self.data[i] * other.data[i];
        }
        
        total
    }

    #[cfg(target_arch = "x86_64")]
    #[target_feature(enable = "sse")]
    unsafe fn dot_product_sse(&self, other: &Self) -> f32 {
        let mut sum = _mm_setzero_ps();
        let chunks = self.data.len() / 4;
        
        for i in 0..chunks {
            let offset = i * 4;
            let a = _mm_loadu_ps(self.data.as_ptr().add(offset));
            let b = _mm_loadu_ps(other.data.as_ptr().add(offset));
            let mul = _mm_mul_ps(a, b);
            sum = _mm_add_ps(sum, mul);
        }
        
        // Horizontal sum of the 4 floats in the SSE register
        let mut result = [0.0f32; 4];
        _mm_storeu_ps(result.as_mut_ptr(), sum);
        let mut total = result.iter().sum::<f32>();
        
        // Handle remaining elements
        for i in (chunks * 4)..self.data.len() {
            total += self.data[i] * other.data[i];
        }
        
        total
    }

    pub fn add(&self, other: &Self) -> Result<Self, VectorError> {
        if self.dimension() != other.dimension() {
            return Err(VectorError::DimensionMismatch {
                expected: self.dimension(),
                actual: other.dimension(),
            });
        }

        let result_data = self.add_simd(other);
        Ok(Self { data: result_data })
    }

    #[cfg(target_arch = "x86_64")]
    fn add_simd(&self, other: &Self) -> Vec<f32> {
        if is_x86_feature_detected!("avx") {
            self.add_avx(other)
        } else if is_x86_feature_detected!("sse") {
            self.add_sse(other)
        } else {
            self.add_scalar(other)
        }
    }

    #[cfg(not(target_arch = "x86_64"))]
    fn add_simd(&self, other: &Self) -> Vec<f32> {
        self.add_scalar(other)
    }

    fn add_scalar(&self, other: &Self) -> Vec<f32> {
        self.data.iter().zip(other.data.iter()).map(|(a, b)| a + b).collect()
    }

    #[cfg(target_arch = "x86_64")]
    #[target_feature(enable = "avx")]
    unsafe fn add_avx(&self, other: &Self) -> Vec<f32> {
        let mut result = vec![0.0f32; self.data.len()];
        let chunks = self.data.len() / 8;
        
        for i in 0..chunks {
            let offset = i * 8;
            let a = _mm256_loadu_ps(self.data.as_ptr().add(offset));
            let b = _mm256_loadu_ps(other.data.as_ptr().add(offset));
            let sum = _mm256_add_ps(a, b);
            _mm256_storeu_ps(result.as_mut_ptr().add(offset), sum);
        }
        
        // Handle remaining elements
        for i in (chunks * 8)..self.data.len() {
            result[i] = self.data[i] + other.data[i];
        }
        
        result
    }

    #[cfg(target_arch = "x86_64")]
    #[target_feature(enable = "sse")]
    unsafe fn add_sse(&self, other: &Self) -> Vec<f32> {
        let mut result = vec![0.0f32; self.data.len()];
        let chunks = self.data.len() / 4;
        
        for i in 0..chunks {
            let offset = i * 4;
            let a = _mm_loadu_ps(self.data.as_ptr().add(offset));
            let b = _mm_loadu_ps(other.data.as_ptr().add(offset));
            let sum = _mm_add_ps(a, b);
            _mm_storeu_ps(result.as_mut_ptr().add(offset), sum);
        }
        
        // Handle remaining elements
        for i in (chunks * 4)..self.data.len() {
            result[i] = self.data[i] + other.data[i];
        }
        
        result
    }

    pub fn subtract(&self, other: &Self) -> Result<Self, VectorError> {
        if self.dimension() != other.dimension() {
            return Err(VectorError::DimensionMismatch {
                expected: self.dimension(),
                actual: other.dimension(),
            });
        }

        let result_data = self.subtract_simd(other);
        Ok(Self { data: result_data })
    }

    #[cfg(target_arch = "x86_64")]
    fn subtract_simd(&self, other: &Self) -> Vec<f32> {
        if is_x86_feature_detected!("avx") {
            self.subtract_avx(other)
        } else if is_x86_feature_detected!("sse") {
            self.subtract_sse(other)
        } else {
            self.subtract_scalar(other)
        }
    }

    #[cfg(not(target_arch = "x86_64"))]
    fn subtract_simd(&self, other: &Self) -> Vec<f32> {
        self.subtract_scalar(other)
    }

    fn subtract_scalar(&self, other: &Self) -> Vec<f32> {
        self.data.iter().zip(other.data.iter()).map(|(a, b)| a - b).collect()
    }

    #[cfg(target_arch = "x86_64")]
    #[target_feature(enable = "avx")]
    unsafe fn subtract_avx(&self, other: &Self) -> Vec<f32> {
        let mut result = vec![0.0f32; self.data.len()];
        let chunks = self.data.len() / 8;
        
        for i in 0..chunks {
            let offset = i * 8;
            let a = _mm256_loadu_ps(self.data.as_ptr().add(offset));
            let b = _mm256_loadu_ps(other.data.as_ptr().add(offset));
            let diff = _mm256_sub_ps(a, b);
            _mm256_storeu_ps(result.as_mut_ptr().add(offset), diff);
        }
        
        // Handle remaining elements
        for i in (chunks * 8)..self.data.len() {
            result[i] = self.data[i] - other.data[i];
        }
        
        result
    }

    #[cfg(target_arch = "x86_64")]
    #[target_feature(enable = "sse")]
    unsafe fn subtract_sse(&self, other: &Self) -> Vec<f32> {
        let mut result = vec![0.0f32; self.data.len()];
        let chunks = self.data.len() / 4;
        
        for i in 0..chunks {
            let offset = i * 4;
            let a = _mm_loadu_ps(self.data.as_ptr().add(offset));
            let b = _mm_loadu_ps(other.data.as_ptr().add(offset));
            let diff = _mm_sub_ps(a, b);
            _mm_storeu_ps(result.as_mut_ptr().add(offset), diff);
        }
        
        // Handle remaining elements
        for i in (chunks * 4)..self.data.len() {
            result[i] = self.data[i] - other.data[i];
        }
        
        result
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

    pub fn manhattan_distance(&self, other: &Self) -> Result<f32, VectorError> {
        if self.dimension() != other.dimension() {
            return Err(VectorError::DimensionMismatch {
                expected: self.dimension(),
                actual: other.dimension(),
            });
        }
        
        let distance: f32 = self.data
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
        
        let max_diff = self.data
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
        
        let count = self.data
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
            return Err(VectorError::InvalidFloat(format!("Invalid p parameter for Minkowski distance: {}", p)));
        }
        
        let sum: f32 = self.data
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
        
        let distance: f32 = self.data
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

    #[test]
    fn test_simd_consistency_large_vectors() {
        // Test with vector size that will use SIMD (32 elements = 4 AVX chunks)
        let data1: Vec<f32> = (0..32).map(|i| i as f32 * 0.1).collect();
        let data2: Vec<f32> = (0..32).map(|i| (i + 1) as f32 * 0.2).collect();
        
        let vec1 = FloatVector::new(data1).unwrap();
        let vec2 = FloatVector::new(data2).unwrap();
        
        // Test dot product
        let dot_result = vec1.dot_product(&vec2).unwrap();
        let expected_dot: f32 = vec1.data.iter().zip(vec2.data.iter()).map(|(a, b)| a * b).sum();
        assert!((dot_result - expected_dot).abs() < 1e-6);
        
        // Test magnitude
        let mag = vec1.magnitude();
        let expected_mag = vec1.data.iter().map(|x| x * x).sum::<f32>().sqrt();
        assert!((mag - expected_mag).abs() < 1e-6);
        
        // Test addition
        let add_result = vec1.add(&vec2).unwrap();
        let expected_add: Vec<f32> = vec1.data.iter().zip(vec2.data.iter()).map(|(a, b)| a + b).collect();
        for (actual, expected) in add_result.data.iter().zip(expected_add.iter()) {
            assert!((actual - expected).abs() < 1e-6);
        }
        
        // Test subtraction
        let sub_result = vec1.subtract(&vec2).unwrap();
        let expected_sub: Vec<f32> = vec1.data.iter().zip(vec2.data.iter()).map(|(a, b)| a - b).collect();
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
        
        // Test that SIMD and scalar results are identical
        let dot_simd = vec1.dot_product(&vec2).unwrap();
        let dot_scalar = vec1.dot_product_scalar(&vec2);
        assert!((dot_simd - dot_scalar).abs() < 1e-6);
    }
}