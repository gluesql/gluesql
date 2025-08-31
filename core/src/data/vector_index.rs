use {
    super::{FloatVector, VectorError},
    rand::Rng,
    serde::{Deserialize, Serialize},
    std::{
        collections::{HashMap, HashSet},
        hash::{Hash, Hasher, DefaultHasher},
    },
};

/// Vector indexing strategies for similarity search optimization
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum VectorIndexType {
    /// No indexing - full scan for all queries
    None,
    /// LSH (Locality-Sensitive Hashing) for approximate similarity search
    LSH {
        num_buckets: usize,
        num_hash_functions: usize,
        similarity_threshold: f32,
    },
    /// Simple range-based indexing for distance queries
    DistanceRange {
        num_buckets: usize,
        max_distance: f32,
    },
}

impl Default for VectorIndexType {
    fn default() -> Self {
        VectorIndexType::LSH {
            num_buckets: 64,
            num_hash_functions: 8,
            similarity_threshold: 0.8,
        }
    }
}

/// LSH (Locality-Sensitive Hashing) index for approximate similarity search
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LSHIndex {
    /// Number of hash buckets
    num_buckets: usize,
    /// Number of hash functions per bucket
    num_hash_functions: usize,
    /// Similarity threshold for candidates
    similarity_threshold: f32,
    /// Hash tables - each table maps hash values to sets of vector IDs
    hash_tables: Vec<HashMap<u64, HashSet<String>>>,
    /// Random projection vectors for LSH
    projection_vectors: Vec<Vec<f32>>,
}

impl LSHIndex {
    pub fn new(num_buckets: usize, num_hash_functions: usize, similarity_threshold: f32, vector_dimension: usize) -> Self {
        let mut projection_vectors = Vec::new();
        
        // Generate random projection vectors for LSH
        let mut rng = rand::thread_rng();
        for _ in 0..num_hash_functions {
            let mut proj = Vec::new();
            for _ in 0..vector_dimension {
                // Simple random values between -1 and 1
                proj.push((rng.gen_range(0.0..1.0) - 0.5) * 2.0);
            }
            projection_vectors.push(proj);
        }

        Self {
            num_buckets,
            num_hash_functions,
            similarity_threshold,
            hash_tables: vec![HashMap::new(); num_hash_functions],
            projection_vectors,
        }
    }

    /// Compute LSH hash for a vector using random projections
    pub fn compute_hash(&self, vector: &FloatVector, hash_function_idx: usize) -> Result<u64, VectorError> {
        if hash_function_idx >= self.num_hash_functions {
            return Err(VectorError::InvalidFloat("Hash function index out of range".to_string()));
        }

        let projection = &self.projection_vectors[hash_function_idx];
        if projection.len() != vector.dimension() {
            return Err(VectorError::DimensionMismatch {
                expected: projection.len(),
                actual: vector.dimension(),
            });
        }

        // Compute dot product with projection vector
        let dot_product: f32 = vector.data()
            .iter()
            .zip(projection.iter())
            .map(|(a, b)| a * b)
            .sum();

        // Convert to hash by quantizing to buckets
        let hash_value = if dot_product >= 0.0 { 1u64 } else { 0u64 };
        
        // Mix with hash function index to avoid collisions
        let mut hasher = DefaultHasher::new();
        hash_value.hash(&mut hasher);
        hash_function_idx.hash(&mut hasher);
        
        Ok(hasher.finish() % (self.num_buckets as u64))
    }

    /// Add a vector to the index
    pub fn add_vector(&mut self, vector_id: String, vector: &FloatVector) -> Result<(), VectorError> {
        for hash_func_idx in 0..self.num_hash_functions {
            let hash_value = self.compute_hash(vector, hash_func_idx)?;
            self.hash_tables[hash_func_idx]
                .entry(hash_value)
                .or_insert_with(HashSet::new)
                .insert(vector_id.clone());
        }
        Ok(())
    }

    /// Remove a vector from the index
    pub fn remove_vector(&mut self, vector_id: &str, vector: &FloatVector) -> Result<(), VectorError> {
        for hash_func_idx in 0..self.num_hash_functions {
            let hash_value = self.compute_hash(vector, hash_func_idx)?;
            if let Some(bucket) = self.hash_tables[hash_func_idx].get_mut(&hash_value) {
                bucket.remove(vector_id);
                if bucket.is_empty() {
                    self.hash_tables[hash_func_idx].remove(&hash_value);
                }
            }
        }
        Ok(())
    }

    /// Find candidate vectors for similarity search
    pub fn find_candidates(&self, query_vector: &FloatVector) -> Result<HashSet<String>, VectorError> {
        let mut candidates = HashSet::new();
        
        for hash_func_idx in 0..self.num_hash_functions {
            let hash_value = self.compute_hash(query_vector, hash_func_idx)?;
            if let Some(bucket) = self.hash_tables[hash_func_idx].get(&hash_value) {
                candidates.extend(bucket.iter().cloned());
            }
        }
        
        Ok(candidates)
    }

    /// Get statistics about the index
    pub fn get_stats(&self) -> IndexStats {
        let total_entries: usize = self.hash_tables
            .iter()
            .map(|table| table.values().map(|bucket| bucket.len()).sum::<usize>())
            .sum();
            
        let total_buckets: usize = self.hash_tables
            .iter()
            .map(|table| table.len())
            .sum();

        IndexStats {
            total_vectors: total_entries / self.num_hash_functions, // Approximate since vectors are in multiple tables
            total_buckets,
            hash_functions: self.num_hash_functions,
            avg_bucket_size: if total_buckets > 0 { total_entries as f32 / total_buckets as f32 } else { 0.0 },
        }
    }
}

/// Distance-based range index for exact similarity queries
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DistanceRangeIndex {
    num_buckets: usize,
    max_distance: f32,
    /// Maps distance bucket to vector IDs
    distance_buckets: HashMap<usize, HashSet<String>>,
    /// Cache of vector magnitudes for faster distance calculation
    vector_magnitudes: HashMap<String, f32>,
}

impl DistanceRangeIndex {
    pub fn new(num_buckets: usize, max_distance: f32) -> Self {
        Self {
            num_buckets,
            max_distance,
            distance_buckets: HashMap::new(),
            vector_magnitudes: HashMap::new(),
        }
    }

    /// Compute distance bucket for a vector relative to origin
    fn compute_distance_bucket(&self, vector: &FloatVector) -> usize {
        let magnitude = vector.magnitude();
        let bucket = ((magnitude / self.max_distance) * (self.num_buckets as f32)) as usize;
        bucket.min(self.num_buckets - 1)
    }

    /// Add vector to distance index
    pub fn add_vector(&mut self, vector_id: String, vector: &FloatVector) {
        let magnitude = vector.magnitude();
        self.vector_magnitudes.insert(vector_id.clone(), magnitude);
        
        let bucket = self.compute_distance_bucket(vector);
        self.distance_buckets
            .entry(bucket)
            .or_insert_with(HashSet::new)
            .insert(vector_id);
    }

    /// Remove vector from distance index
    pub fn remove_vector(&mut self, vector_id: &str, vector: &FloatVector) {
        self.vector_magnitudes.remove(vector_id);
        
        let bucket = self.compute_distance_bucket(vector);
        if let Some(bucket_set) = self.distance_buckets.get_mut(&bucket) {
            bucket_set.remove(vector_id);
            if bucket_set.is_empty() {
                self.distance_buckets.remove(&bucket);
            }
        }
    }

    /// Find vectors within a distance range
    pub fn find_in_range(&self, query_vector: &FloatVector, max_distance: f32) -> HashSet<String> {
        let query_magnitude = query_vector.magnitude();
        let mut candidates = HashSet::new();
        
        // Check buckets that could contain vectors within the distance range
        let min_target_magnitude = (query_magnitude - max_distance).max(0.0);
        let max_target_magnitude = query_magnitude + max_distance;
        
        let min_bucket = ((min_target_magnitude / self.max_distance) * (self.num_buckets as f32)) as usize;
        let max_bucket = ((max_target_magnitude / self.max_distance) * (self.num_buckets as f32)) as usize;
        
        for bucket in min_bucket..=max_bucket.min(self.num_buckets - 1) {
            if let Some(bucket_set) = self.distance_buckets.get(&bucket) {
                candidates.extend(bucket_set.iter().cloned());
            }
        }
        
        candidates
    }

    /// Get cached magnitude for a vector
    pub fn get_magnitude(&self, vector_id: &str) -> Option<f32> {
        self.vector_magnitudes.get(vector_id).copied()
    }
}

/// Statistics about vector index performance
#[derive(Debug, Clone)]
pub struct IndexStats {
    pub total_vectors: usize,
    pub total_buckets: usize,
    pub hash_functions: usize,
    pub avg_bucket_size: f32,
}

/// Complete vector index combining multiple indexing strategies
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VectorIndex {
    index_type: VectorIndexType,
    lsh_index: Option<LSHIndex>,
    distance_index: Option<DistanceRangeIndex>,
    vector_dimension: usize,
}

impl VectorIndex {
    pub fn new(index_type: VectorIndexType, vector_dimension: usize) -> Self {
        let (lsh_index, distance_index) = match &index_type {
            VectorIndexType::None => (None, None),
            VectorIndexType::LSH { num_buckets, num_hash_functions, similarity_threshold } => {
                let lsh = LSHIndex::new(*num_buckets, *num_hash_functions, *similarity_threshold, vector_dimension);
                (Some(lsh), None)
            }
            VectorIndexType::DistanceRange { num_buckets, max_distance } => {
                let dist = DistanceRangeIndex::new(*num_buckets, *max_distance);
                (None, Some(dist))
            }
        };

        Self {
            index_type,
            lsh_index,
            distance_index,
            vector_dimension,
        }
    }

    /// Add a vector to the appropriate index
    pub fn add_vector(&mut self, vector_id: String, vector: &FloatVector) -> Result<(), VectorError> {
        if vector.dimension() != self.vector_dimension {
            return Err(VectorError::DimensionMismatch {
                expected: self.vector_dimension,
                actual: vector.dimension(),
            });
        }

        if let Some(ref mut lsh) = self.lsh_index {
            lsh.add_vector(vector_id.clone(), vector)?;
        }
        
        if let Some(ref mut dist) = self.distance_index {
            dist.add_vector(vector_id, vector);
        }
        
        Ok(())
    }

    /// Remove a vector from the index
    pub fn remove_vector(&mut self, vector_id: &str, vector: &FloatVector) -> Result<(), VectorError> {
        if let Some(ref mut lsh) = self.lsh_index {
            lsh.remove_vector(vector_id, vector)?;
        }
        
        if let Some(ref mut dist) = self.distance_index {
            dist.remove_vector(vector_id, vector);
        }
        
        Ok(())
    }

    /// Find candidate vectors for similarity search
    pub fn find_similarity_candidates(&self, query_vector: &FloatVector) -> Result<Vec<String>, VectorError> {
        match &self.lsh_index {
            Some(lsh) => {
                let candidates = lsh.find_candidates(query_vector)?;
                Ok(candidates.into_iter().collect())
            }
            None => {
                // No indexing - return empty (caller should do full scan)
                Ok(Vec::new())
            }
        }
    }

    /// Find vectors within a distance range
    pub fn find_distance_candidates(&self, query_vector: &FloatVector, max_distance: f32) -> Vec<String> {
        match &self.distance_index {
            Some(dist) => {
                let candidates = dist.find_in_range(query_vector, max_distance);
                candidates.into_iter().collect()
            }
            None => {
                // No distance indexing - return empty (caller should do full scan)
                Vec::new()
            }
        }
    }

    /// Get index statistics
    pub fn get_stats(&self) -> Option<IndexStats> {
        self.lsh_index.as_ref().map(|lsh| lsh.get_stats())
    }

    /// Get the index type
    pub fn index_type(&self) -> &VectorIndexType {
        &self.index_type
    }

    /// Check if indexing is enabled
    pub fn is_indexed(&self) -> bool {
        !matches!(self.index_type, VectorIndexType::None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lsh_index_creation() {
        let index = LSHIndex::new(32, 4, 0.8, 3);
        assert_eq!(index.num_buckets, 32);
        assert_eq!(index.num_hash_functions, 4);
        assert_eq!(index.projection_vectors.len(), 4);
        assert_eq!(index.projection_vectors[0].len(), 3);
    }

    #[test]
    fn test_lsh_add_and_find() -> Result<(), VectorError> {
        let mut index = LSHIndex::new(16, 4, 0.8, 3);
        
        let vec1 = FloatVector::new(vec![1.0, 2.0, 3.0])?;
        let vec2 = FloatVector::new(vec![1.1, 2.1, 3.1])?;
        let vec3 = FloatVector::new(vec![10.0, 20.0, 30.0])?;
        
        index.add_vector("v1".to_string(), &vec1)?;
        index.add_vector("v2".to_string(), &vec2)?;
        index.add_vector("v3".to_string(), &vec3)?;
        
        let candidates = index.find_candidates(&vec1)?;
        assert!(candidates.contains("v1"));
        
        Ok(())
    }

    #[test]
    fn test_distance_range_index() -> Result<(), VectorError> {
        let mut index = DistanceRangeIndex::new(10, 10.0);
        
        let vec1 = FloatVector::new(vec![1.0, 0.0, 0.0])?; // magnitude = 1.0
        let vec2 = FloatVector::new(vec![2.0, 0.0, 0.0])?; // magnitude = 2.0
        let vec3 = FloatVector::new(vec![5.0, 0.0, 0.0])?; // magnitude = 5.0
        
        index.add_vector("v1".to_string(), &vec1);
        index.add_vector("v2".to_string(), &vec2);
        index.add_vector("v3".to_string(), &vec3);
        
        let query = FloatVector::new(vec![1.5, 0.0, 0.0])?; // magnitude = 1.5
        let candidates = index.find_in_range(&query, 1.0);
        
        assert!(candidates.contains("v1"));
        assert!(candidates.contains("v2"));
        assert!(!candidates.contains("v3")); // Too far
        
        Ok(())
    }

    #[test]
    fn test_vector_index_integration() -> Result<(), VectorError> {
        let index_type = VectorIndexType::LSH {
            num_buckets: 16,
            num_hash_functions: 4,
            similarity_threshold: 0.8,
        };
        
        let mut index = VectorIndex::new(index_type, 3);
        
        let vec1 = FloatVector::new(vec![1.0, 2.0, 3.0])?;
        index.add_vector("test_vector".to_string(), &vec1)?;
        
        let candidates = index.find_similarity_candidates(&vec1)?;
        assert!(candidates.contains(&"test_vector".to_string()));
        
        Ok(())
    }
}