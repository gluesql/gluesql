package org.gluesql.client.kotlin

import kotlinx.coroutines.*
import org.gluesql.uniffi.Storage
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
}
