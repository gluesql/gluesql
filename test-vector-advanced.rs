use gluesql_core::prelude::*;
use gluesql_memory_storage::MemoryStorage;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let storage = MemoryStorage::default();
    let mut glue = Glue::new(storage);

    // Create a table with vector column
    glue.execute("CREATE TABLE test_vectors (id INTEGER, vec FLOAT_VECTOR)").await?;
    
    // Insert test data
    glue.execute("INSERT INTO test_vectors VALUES (1, '[1.0, 2.0, 3.0]')").await?;
    
    println!("Testing basic functions first...");
    
    // Test basic vector function that should work
    match glue.execute("SELECT VECTOR_MAGNITUDE('[3.0, 4.0]') as magnitude").await {
        Ok(payload) => println!("VECTOR_MAGNITUDE works: {:?}", payload),
        Err(e) => println!("VECTOR_MAGNITUDE failed: {:?}", e),
    }
    
    println!("Testing advanced distance functions...");
    
    // Test advanced distance function
    match glue.execute("SELECT VECTOR_MANHATTAN_DIST('[1.0, 2.0, 3.0]', '[4.0, 6.0, 8.0]') as manhattan").await {
        Ok(payload) => println!("VECTOR_MANHATTAN_DIST works: {:?}", payload),
        Err(e) => println!("VECTOR_MANHATTAN_DIST failed: {:?}", e),
    }
    
    Ok(())
}