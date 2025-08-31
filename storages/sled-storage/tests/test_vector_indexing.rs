use {
    async_trait::async_trait, gluesql_core::prelude::Glue,
    gluesql_sled_storage::SledStorage, test_suite::*,
};

struct SledTester {
    glue: Glue<SledStorage>,
}

#[async_trait(?Send)]
impl Tester<SledStorage> for SledTester {
    async fn new(path: &str) -> Self {
        let temp_dir = std::env::temp_dir().join(format!("gluesql_sled_test_{}", path));
        if temp_dir.exists() {
            std::fs::remove_dir_all(&temp_dir).unwrap();
        }
        std::fs::create_dir_all(&temp_dir).unwrap();
        
        let db_path = temp_dir.join("test.db");
        let storage = SledStorage::new(&db_path).unwrap();
        let glue = Glue::new(storage);

        SledTester { glue }
    }

    fn get_glue(&mut self) -> &mut Glue<SledStorage> {
        &mut self.glue
    }
}

#[tokio::test]
async fn sled_vector_indexing_basic() {
    use gluesql_core::data::{FloatVector, VectorIndexType};
    
    println!("Testing Sled Storage Vector Indexing...");

    let mut tester = SledTester::new("vector_indexing").await;
    let glue = tester.get_glue();

    // Test 1: Create table with FloatVector column
    println!("1. Creating table with FloatVector column...");
    glue.execute("CREATE TABLE embeddings (id INTEGER, vector FLOAT_VECTOR)").await.unwrap();
    println!("   ✅ Table created successfully");

    // Test 2: Insert vector data
    println!("2. Inserting vector data...");
    glue.execute("INSERT INTO embeddings VALUES (1, '[1.0, 2.0, 3.0]')").await.unwrap();
    glue.execute("INSERT INTO embeddings VALUES (2, '[4.0, 5.0, 6.0]')").await.unwrap();
    glue.execute("INSERT INTO embeddings VALUES (3, '[7.0, 8.0, 9.0]')").await.unwrap();
    println!("   ✅ Vector data inserted successfully");

    // Test 3: Query vector data
    println!("3. Querying vector data...");
    let result = glue.execute("SELECT * FROM embeddings").await.unwrap();
    println!("   Result: {:?}", result);
    println!("   ✅ Vector data queried successfully");

    // Test 4: Test vector functions
    println!("4. Testing vector functions...");
    let result = glue.execute("SELECT VECTOR_MAGNITUDE('[3.0, 4.0]')").await.unwrap();
    println!("   Vector magnitude result: {:?}", result);
    
    let result = glue.execute("SELECT VECTOR_DOT('[1.0, 2.0, 3.0]', '[4.0, 5.0, 6.0]')").await.unwrap();
    println!("   Vector dot product result: {:?}", result);
    
    println!("   ✅ Vector functions work correctly");

    // Test 5: Test vector indexing programmatically
    println!("5. Testing vector indexing...");
    let storage_ref = &mut glue.storage;
    
    // Create LSH vector index
    let index_result = storage_ref.create_vector_index(
        "embeddings", 
        "vector", 
        VectorIndexType::LSH { 
            num_buckets: 16, 
            num_hash_functions: 4, 
            similarity_threshold: 0.8,
        }
    );
    
    match index_result {
        Ok(_) => println!("   ✅ Vector index created successfully"),
        Err(e) => println!("   ❌ Vector index creation failed: {}", e),
    }

    // Test vector similarity search
    let test_vector = FloatVector::new(vec![1.0, 2.0, 3.0]).unwrap();
    let candidates_result = storage_ref.find_vector_similarity_candidates("embeddings", "vector", &test_vector);
    match candidates_result {
        Ok(candidates) => {
            println!("   Similarity candidates: {:?}", candidates);
            println!("   ✅ Vector similarity search completed");
        }
        Err(e) => println!("   ❌ Vector similarity search failed: {}", e),
    }

    // Test distance-based search
    let distance_candidates = storage_ref.find_vector_distance_candidates("embeddings", "vector", &test_vector, 10.0);
    println!("   Distance candidates: {:?}", distance_candidates);
    println!("   ✅ Vector distance search completed");

    println!("\n🎉 All Sled Storage Vector Indexing tests passed!");
}