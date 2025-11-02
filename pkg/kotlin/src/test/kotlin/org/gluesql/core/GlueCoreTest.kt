package org.gluesql.core

import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

/**
 * Basic tests for GlueSQL Core UniFFI bindings.
 *
 * These tests verify that the native library loads correctly and
 * basic operations work as expected.
 */
class GlueCoreTest {
    @Test
    fun `test memory storage creation`() {
        val storage = Storage.Memory
        assertNotNull(storage, "Memory storage should be created")
    }

    @Test
    fun `test glue creation`() {
        val storage = Storage.Memory
        val glue = Glue(storage)
        assertNotNull(glue, "Glue instance should be created")
    }

    @Test
    fun `test basic query`() =
        runBlocking {
            val storage = Storage.Memory
            val glue = Glue(storage)

            // Create table
            val createResult = glue.query("CREATE TABLE users (id INTEGER, name TEXT)")
            assertNotNull(createResult, "Create table result should not be null")
            assertEquals(1, createResult.size, "Should return one result")
            assertTrue(createResult[0] is QueryResult.Create, "Should be CreateTable result")

            // Insert data
            val insertResult = glue.query("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')")
            assertNotNull(insertResult, "Insert result should not be null")
            assertEquals(1, insertResult.size, "Should return one result")
            assertTrue(insertResult[0] is QueryResult.Insert, "Should be Insert result")

            // Select data
            val selectResult = glue.query("SELECT * FROM users")
            assertNotNull(selectResult, "Select result should not be null")
            assertEquals(1, selectResult.size, "Should return one result")
            assertTrue(selectResult[0] is QueryResult.Select, "Should be Select result")

            val select = selectResult[0] as QueryResult.Select
            assertEquals(2, select.result.rows.size, "Should have 2 rows")
        }

    @Test
    fun `test json storage creation`() {
        // Just test that we can create JSON storage (without queries)
        val tempPath = "${System.getProperty("java.io.tmpdir")}/test_gluesql_${System.currentTimeMillis()}.json"
        val storage = Storage.Json(tempPath)
        assertNotNull(storage, "JSON storage should be created")
    }

    @Test
    fun `test sled storage creation`() {
        // Just test that we can create Sled storage (without queries)
        val tempPath = "${System.getProperty("java.io.tmpdir")}/test_gluesql_sled_${System.currentTimeMillis()}"
        val config =
            SledConfig(
                path = tempPath,
                // 1MB cache
                cacheCapacity = 1024L * 1024L,
                mode = Mode.LOW_SPACE,
                createNew = true,
                temporary = true,
                useCompression = false,
                compressionFactor = 5,
                printProfileOnDrop = false,
            )
        val storage = Storage.Sled(config)
        assertNotNull(storage, "Sled storage should be created")
    }

    @Test
    fun `test shared memory storage`() =
        runBlocking {
            val storage = Storage.SharedMemory
            val glue = Glue(storage)

            val result = glue.query("CREATE TABLE test (id INTEGER)")
            assertNotNull(result, "Query should execute successfully")
        }

    @Test
    fun `test sql value types`() =
        runBlocking {
            val storage = Storage.Memory
            val glue = Glue(storage)

            glue.query(
                """CREATE TABLE types_test (
                id INTEGER,
                name TEXT,
                active BOOLEAN,
                score FLOAT
            )""",
            )

            glue.query("INSERT INTO types_test VALUES (1, 'test', TRUE, 3.14)")

            val result = glue.query("SELECT * FROM types_test")
            val select = result[0] as QueryResult.Select

            assertEquals(1, select.result.rows.size, "Should have 1 row")

            val row = select.result.rows[0]
            assertEquals(4, row.size, "Row should have 4 columns")
        }

    @Test
    fun `test error handling - non-existent table`() {
        val storage = Storage.Memory
        val glue = Glue(storage)

        // Query non-existent table should throw exception
        assertThrows(GlueSqlException::class.java) {
            runBlocking {
                glue.query("SELECT * FROM non_existent_table")
            }
        }
    }

    @Test
    fun `test error handling - invalid sql`() {
        val storage = Storage.Memory
        val glue = Glue(storage)

        // Invalid SQL should throw exception
        assertThrows(GlueSqlException::class.java) {
            runBlocking {
                glue.query("INVALID SQL SYNTAX")
            }
        }
    }

    @Test
    fun `test multiple queries`() =
        runBlocking {
            val storage = Storage.Memory
            val glue = Glue(storage)

            // Create multiple tables
            glue.query("CREATE TABLE users (id INTEGER)")
            glue.query("CREATE TABLE posts (id INTEGER, user_id INTEGER)")

            // Insert into both
            glue.query("INSERT INTO users VALUES (1)")
            glue.query("INSERT INTO posts VALUES (1, 1)")

            // Query both
            val usersResult = glue.query("SELECT * FROM users")
            val postsResult = glue.query("SELECT * FROM posts")

            assertEquals(1, (usersResult[0] as QueryResult.Select).result.rows.size)
            assertEquals(1, (postsResult[0] as QueryResult.Select).result.rows.size)
        }
}
