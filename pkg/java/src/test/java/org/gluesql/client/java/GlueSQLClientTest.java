package org.gluesql.client.java;

import org.gluesql.uniffi.QueryResult;
import org.gluesql.uniffi.Storage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Simple JUnit tests demonstrating Java GlueSQLClient usage.
 */
public class GlueSQLClientTest {
    
    private GlueSQLClient client;
    
    @BeforeEach
    void setUp() {
        Storage storage = StorageFactory.memory();
        client = new GlueSQLClient(storage);
    }
    
    @Test
    void testSynchronousQuery() throws Exception {
        // Create table
        List<QueryResult> createResult = client.query("CREATE TABLE users (id INTEGER, name TEXT)");
        assertNotNull(createResult);
        
        // Insert data
        List<QueryResult> insertResult = client.query("INSERT INTO users VALUES (1, 'Alice')");
        assertNotNull(insertResult);
        
        // Query data
        List<QueryResult> selectResult = client.query("SELECT * FROM users");
        assertNotNull(selectResult);
        assertEquals(1, selectResult.size());
    }
    
    @Test
    void testAsynchronousQuery() throws Exception {
        // Create table asynchronously
        CompletableFuture<List<QueryResult>> future = client.queryAsync("CREATE TABLE products (id INTEGER, name TEXT)");
        
        List<QueryResult> result = future.get(5, TimeUnit.SECONDS);
        assertNotNull(result);
        
        // Insert data asynchronously
        CompletableFuture<List<QueryResult>> insertFuture = client.queryAsync("INSERT INTO products VALUES (1, 'Laptop')");
        List<QueryResult> insertResult = insertFuture.get(5, TimeUnit.SECONDS);
        assertNotNull(insertResult);
    }
    
    @Test
    void testStorageFactory() {
        // Test different storage types creation
        Storage memoryStorage = StorageFactory.memory();
        assertNotNull(memoryStorage);
        
        Storage jsonStorage = StorageFactory.json("/tmp/test.json");
        assertNotNull(jsonStorage);
        
        Storage sharedMemoryStorage = StorageFactory.sharedMemory("test");
        assertNotNull(sharedMemoryStorage);
    }
}
