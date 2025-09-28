package org.gluesql.client.kotlin

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import org.gluesql.storage.StorageFactory.Companion.json
import org.gluesql.storage.StorageFactory.Companion.memory
import org.gluesql.storage.StorageFactory.Companion.sharedMemory
import org.gluesql.storage.StorageFactory.Companion.sled
import org.gluesql.uniffi.Mode
import org.gluesql.uniffi.SledConfig
import org.gluesql.uniffi.Storage
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

/**
 * Simple JUnit tests demonstrating Kotlin GlueSQLClient usage with coroutines.
 */
class GlueSQLClientTest {
    
    @Test
    fun `test basic coroutine query`() {
        runBlocking {
            val client = GlueSQLClient(Storage.Memory)
            
            // Create table
            val createResult = client.query("CREATE TABLE users (id INTEGER, name TEXT)")
            assertNotNull(createResult)
            
            // Insert data
            val insertResult = client.query("INSERT INTO users VALUES (1, 'Alice')")
            assertNotNull(insertResult)
            
            // Query data
            val selectResult = client.query("SELECT * FROM users")
            assertNotNull(selectResult)
            assertEquals(1, selectResult.size)
        }
    }
    
    @Test
    fun `test storage factory`() {
        val memoryStorage = Storage.Memory
        assertNotNull(memoryStorage)
        
        val client = GlueSQLClient(memoryStorage)
        assertNotNull(client)
    }
    
    @Test
    fun `test custom coroutine context`() {
        runBlocking {
            // Test with custom dispatcher
            val customClient = GlueSQLClient(Storage.Memory, Dispatchers.Default)
            
            val result = customClient.query("CREATE TABLE test (id INTEGER)")
            assertNotNull(result)
        }
    }

    @Test
    fun testStorageFactory() {
        // Test different storage types creation
        val memoryStorage = memory()
        Assertions.assertNotNull(memoryStorage)

        val jsonStorage = json("/tmp/test.json")
        Assertions.assertNotNull(jsonStorage)

        val sharedMemoryStorage = sharedMemory()
        Assertions.assertNotNull(sharedMemoryStorage)

        val sledConfig = SledConfig(
            path = "/tmp/test-advanced.sled",
            cacheCapacity = 2048L,
            mode = Mode.HIGH_THROUGHPUT,
            createNew = true,
            temporary = false,
            useCompression = true,
            compressionFactor = 7,
            printProfileOnDrop = false
        )
        val advancedSledStorage = sled(sledConfig)
        Assertions.assertNotNull(advancedSledStorage)
    }
}
