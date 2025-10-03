# Float Vector Implementation Plan for GlueSQL

## Overview

This document outlines a comprehensive implementation plan for adding float-number vector support to GlueSQL. The implementation will enable efficient storage, manipulation, and querying of multi-dimensional numeric vectors, supporting common vector operations like addition, scalar multiplication, dot products, and distance calculations.

## Current State Analysis

**Current Branch**: `add-vector-feature` (dedicated branch for vector implementation)
**Related Code**: 
- Existing `Vector<T>` utility in `utils/src/vector.rs`
- `List` data type implementation as a model (`ast/data_type.rs:31`, `data/value.rs:65`)
- Function system in `ast/function.rs` and `executor/evaluate/function.rs`

## 1. Data Type Definition

### 1.1 Core Data Type Addition

**Files to Modify:**
- `core/src/ast/data_type.rs:8-34`
- `core/src/data/value.rs:41-68`

**Implementation:**
```rust
// In ast/data_type.rs
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, Display)]
#[strum(serialize_all = "SCREAMING_SNAKE_CASE")]
pub enum DataType {
    // ... existing types
    Point,
    FloatVector,  // New: For float vectors
    // Potential future extensions:
    // IntVector,   // For integer vectors  
    // DoubleVector, // For double precision vectors
}

// In data/value.rs  
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Value {
    // ... existing values
    Point(Point),
    FloatVector(Vec<f32>),  // New: Storage for float vectors
    Null,
}
```

### 1.2 Vector Constraints and Validation

**New File**: `core/src/data/float_vector.rs`

**Features:**
- Maximum dimension limits (e.g., 1024 dimensions)
- Validation for NaN and infinite values
- Memory usage tracking
- Normalization utilities

```rust
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FloatVector {
    data: Vec<f32>,
    dimension: usize,
}

impl FloatVector {
    pub const MAX_DIMENSIONS: usize = 1024;
    
    pub fn new(data: Vec<f32>) -> Result<Self, VectorError> {
        if data.len() > Self::MAX_DIMENSIONS {
            return Err(VectorError::TooManyDimensions);
        }
        
        if data.iter().any(|f| f.is_nan() || f.is_infinite()) {
            return Err(VectorError::InvalidFloat);
        }
        
        Ok(Self {
            dimension: data.len(),
            data,
        })
    }
    
    pub fn dimension(&self) -> usize { self.dimension }
    pub fn data(&self) -> &[f32] { &self.data }
    pub fn magnitude(&self) -> f32 { /* implementation */ }
    pub fn normalize(&self) -> Self { /* implementation */ }
}
```

## 2. SQL Syntax Extensions

### 2.1 Table Definition Syntax

**Files to Modify:**
- SQL parser components
- `core/src/translate/ddl.rs`

**SQL Examples:**
```sql
-- Basic vector column
CREATE TABLE embeddings (
    id INTEGER PRIMARY KEY,
    vector FLOAT_VECTOR
);

-- Vector with dimension constraint (future enhancement)
CREATE TABLE embeddings_512 (
    id INTEGER PRIMARY KEY,  
    vector FLOAT_VECTOR(512)  -- 512-dimensional vectors
);
```

### 2.2 Data Insertion and Querying

**Insert Syntax:**
```sql
-- Array literal syntax
INSERT INTO embeddings VALUES 
    (1, '[0.1, 0.2, 0.3, 0.4]'),
    (2, '[0.5, 0.6, 0.7, 0.8]');

-- Function-based construction
INSERT INTO embeddings VALUES 
    (3, VECTOR(0.1, 0.2, 0.3, 0.4));
```

**Query Syntax:**
```sql  
-- Vector element access
SELECT id, vector[0] as first_element FROM embeddings;

-- Vector operations
SELECT id, 
       VECTOR_MAGNITUDE(vector) as magnitude,
       VECTOR_DOT(vector, '[1.0, 1.0, 1.0, 1.0]') as dot_product
FROM embeddings;
```

## 3. Vector Operations

### 3.1 Core Mathematical Operations

**Files to Create/Modify:**
- `core/src/executor/evaluate/function/vector.rs` (new)
- `core/src/ast/function.rs` (extend enum)

**Operations to Implement:**

#### Basic Arithmetic
```rust
pub enum VectorFunction {
    // Element-wise operations
    VectorAdd(Expr, Expr),           // v1 + v2
    VectorSub(Expr, Expr),           // v1 - v2
    VectorMul(Expr, Expr),           // v1 * v2 (element-wise)
    VectorScalarMul(Expr, Expr),     // v * scalar
    
    // Vector-specific operations  
    VectorDot(Expr, Expr),           // dot product
    VectorCross(Expr, Expr),         // cross product (3D only)
    VectorMagnitude(Expr),           // ||v||
    VectorNormalize(Expr),           // v / ||v||
    
    // Distance metrics
    VectorEuclideanDist(Expr, Expr), // Euclidean distance
    VectorManhattanDist(Expr, Expr), // Manhattan distance  
    VectorCosineSim(Expr, Expr),     // Cosine similarity
    
    // Utility functions
    VectorDimension(Expr),           // Get vector dimension
    VectorAt(Expr, Expr),            // Get element at index
}
```

#### SQL Function Examples
```sql
-- Vector arithmetic
SELECT VECTOR_ADD(v1, v2) FROM vectors;
SELECT VECTOR_SCALAR_MUL(v1, 2.0) FROM vectors;

-- Similarity/distance operations
SELECT id1, id2, VECTOR_COSINE_SIM(v1, v2) as similarity
FROM vectors v1 CROSS JOIN vectors v2 
WHERE v1.id != v2.id;

-- Find nearest neighbors
SELECT id, VECTOR_EUCLIDEAN_DIST(vector, '[0.5, 0.5, 0.5]') as distance
FROM embeddings 
ORDER BY distance 
LIMIT 10;
```

### 3.2 Advanced Operations (Future)

**Phase 2 Features:**
- Matrix operations (when vectors represent matrices)  
- Dimensionality reduction operations
- Vector quantization functions
- Approximate nearest neighbor functions

## 4. Storage Implementation

### 4.1 Serialization Strategy

**Primary Approach**: Binary serialization for efficiency
**Fallback**: JSON for human-readable formats

```rust
// In data/value.rs - extend serialization
impl Value {
    pub fn serialize_binary(&self) -> Vec<u8> {
        match self {
            Value::FloatVector(vec) => {
                let mut bytes = Vec::new();
                bytes.extend_from_slice(&(vec.len() as u32).to_le_bytes());
                for f in vec {
                    bytes.extend_from_slice(&f.to_le_bytes());
                }
                bytes
            }
            // ... other types
        }
    }
}
```

### 4.2 Storage Backend Considerations

**Memory Storage**: 
- Direct `Vec<f32>` storage
- Memory pooling for frequent allocations/deallocations

**Persistent Storage (Sled/Redb)**:
- Binary encoding for space efficiency
- Potential compression for large vectors

**JSON Storage**:  
- Array representation: `[0.1, 0.2, 0.3]`
- Metadata for dimension tracking

**Parquet Storage**:
- Native array support with efficient columnar storage
- Automatic compression

### 4.3 Indexing Strategy

**Files to Modify:**
- `core/src/store/index.rs`
- Storage-specific index implementations

**Indexing Approaches:**
1. **Hash-based**: For exact vector matching (rare)
2. **LSH (Locality-Sensitive Hashing)**: For approximate similarity
3. **Tree-based**: For range queries on specific dimensions
4. **Composite**: Combine multiple indexing strategies

```rust
pub enum VectorIndexType {
    None,           // No indexing
    Exact,          // Hash-based exact matching
    LSH {           // Locality-sensitive hashing
        buckets: usize,
        hash_functions: usize,
    },
    Annoy {         // Approximate nearest neighbors
        trees: usize,
    },
}
```

## 5. Performance Considerations

### 5.1 Memory Optimization

**Strategies:**
- **SIMD Instructions**: Use platform-specific SIMD for vector operations
- **Memory Alignment**: Align vectors for better cache performance  
- **Lazy Loading**: Load vectors on-demand for large datasets
- **Vector Pooling**: Reuse allocated vectors to reduce GC pressure

**Implementation:**
```rust
// Use SIMD-friendly operations
#[cfg(target_arch = "x86_64")]
use std::arch::x86_64::*;

impl FloatVector {
    pub fn dot_product_simd(&self, other: &Self) -> f32 {
        // SIMD-optimized dot product implementation
        // Fall back to standard implementation on unsupported platforms
    }
}
```

### 5.2 Query Performance

**Optimizations:**
- **Early termination**: Stop calculations when distance thresholds exceeded
- **Batch operations**: Process multiple vectors simultaneously  
- **Parallel processing**: Use rayon for CPU-intensive operations
- **Query planning**: Optimize vector operations in query planner

### 5.3 Scalability

**Large Dataset Handling:**
- **Streaming operations**: Process vectors without loading all into memory
- **Disk-based operations**: Spill to disk for memory-limited scenarios
- **Distributed processing**: Prepare for future distributed query support

**Benchmarking Plan:**
- Vector sizes: 128, 256, 512, 1024 dimensions
- Dataset sizes: 1K, 10K, 100K, 1M vectors
- Operations: dot product, cosine similarity, k-NN search
- Memory usage profiling
- Query latency measurements

## 6. Implementation Phases

### Phase 1: Core Infrastructure (Weeks 1-2)
- [ ] Add `FloatVector` data type to AST and Value enums
- [ ] Implement basic serialization/deserialization  
- [ ] Add SQL parsing for vector literals
- [ ] Create basic vector validation
- [ ] Update memory storage to handle vectors

### Phase 2: Basic Operations (Weeks 3-4)  
- [ ] Implement core mathematical operations
- [ ] Add vector functions to executor
- [ ] Create comprehensive test suite
- [ ] Update storage backends (JSON, CSV)
- [ ] Add basic indexing support

### Phase 3: Advanced Features (Weeks 5-6)
- [ ] Optimize performance with SIMD
- [ ] Implement advanced distance metrics
- [ ] Add persistent storage support (Sled, Parquet)
- [ ] Create vector-specific indexes
- [ ] Add query optimization for vector operations

### Phase 4: Polish and Production (Weeks 7-8)
- [ ] Comprehensive benchmarking
- [ ] Documentation and examples
- [ ] Integration with existing test suite
- [ ] Language binding updates (JavaScript, Python)
- [ ] Performance tuning based on benchmarks

## 7. Testing Strategy

### 7.1 Unit Tests
**Files to Create:**
- `test-suite/src/data_type/float_vector.rs`
- `test-suite/src/function/vector_operations.rs`

**Test Categories:**
- Data type validation and edge cases
- Serialization/deserialization correctness
- Mathematical operation accuracy
- Performance regression tests

### 7.2 Integration Tests
- Cross-storage compatibility
- Complex query scenarios  
- Vector operations in JOINs and aggregations
- Memory leak detection

### 7.3 Performance Tests
- Benchmark against reference implementations
- Memory usage profiling
- Query latency under load
- Scalability limits

## 8. Documentation Plan

### 8.1 Technical Documentation
- API reference for vector functions
- Storage format specifications
- Performance characteristics
- Migration guides

### 8.2 User Documentation  
- Tutorial: "Getting Started with Vectors"
- Example applications (recommendation systems, similarity search)
- Best practices for vector operations
- Troubleshooting guide

## 9. Migration and Compatibility

### 9.1 Backward Compatibility
- No breaking changes to existing functionality
- Graceful handling of unsupported operations in old storage formats
- Version detection for storage format upgrades

### 9.2 Storage Migration
- Automatic detection of vector columns in existing tables
- Batch conversion tools for large datasets
- Rollback procedures for failed migrations

## 10. Success Metrics

### 10.1 Functional Metrics
- [ ] Support for all planned vector operations
- [ ] Cross-storage backend compatibility  
- [ ] Memory usage within acceptable limits (< 2x overhead)
- [ ] Query performance competitive with specialized vector databases

### 10.2 Quality Metrics
- [ ] >95% test coverage for vector-related code
- [ ] Zero memory leaks in continuous operation
- [ ] Successful integration with existing GlueSQL features
- [ ] Positive community feedback and adoption

---

## Conclusion

This implementation plan provides a comprehensive roadmap for adding robust float vector support to GlueSQL. The phased approach ensures incremental progress while maintaining system stability. The design leverages existing GlueSQL patterns and infrastructure, minimizing risk while maximizing functionality.

The vector support will position GlueSQL as a competitive option for applications requiring both traditional SQL operations and modern vector/similarity computations, such as machine learning, recommendation systems, and semantic search applications.

**Next Steps**: Begin Phase 1 implementation, starting with core data type definitions and basic storage support.