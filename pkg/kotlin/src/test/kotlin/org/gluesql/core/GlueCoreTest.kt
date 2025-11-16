package org.gluesql.core

import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource
import java.io.File
import java.util.stream.Stream

/**
 * Basic tests for GlueSQL Core UniFFI bindings.
 *
 * These tests verify that the native library loads correctly and
 * basic operations work as expected across all storage backends.
 */
class GlueCoreTest {
    companion object {
        /**
         * Storage wrapper that handles cleanup for file-based storages.
         */
        data class StorageFixture(
            val storage: Storage,
            val name: String,
            private val cleanup: () -> Unit = {},
        ) : AutoCloseable {
            override fun close() = cleanup()

            override fun toString(): String = name
        }

        /**
         * Provides all 4 storage implementations for parameterized tests.
         */
        @JvmStatic
        fun storageProvider(): Stream<StorageFixture> {
            val timestamp = System.currentTimeMillis()
            val tempDir = System.getProperty("java.io.tmpdir")

            return Stream.of(
                StorageFixture(
                    storage = Storage.Memory,
                    name = "Memory",
                ),
                StorageFixture(
                    storage =
                        Storage.Json("$tempDir/test_gluesql_json_$timestamp.json"),
                    name = "Json",
                    cleanup = {
                        File("$tempDir/test_gluesql_json_$timestamp.json").deleteRecursively()
                    },
                ),
                StorageFixture(
                    storage =
                        Storage.Sled(
                            SledConfig(
                                path = "$tempDir/test_gluesql_sled_$timestamp",
                                cacheCapacity = 1024L * 1024L,
                                mode = Mode.LOW_SPACE,
                                createNew = true,
                                temporary = true,
                                useCompression = false,
                                compressionFactor = 5,
                                printProfileOnDrop = false,
                            ),
                        ),
                    name = "Sled",
                    cleanup = {
                        File("$tempDir/test_gluesql_sled_$timestamp").deleteRecursively()
                    },
                ),
                StorageFixture(
                    storage = Storage.SharedMemory,
                    name = "SharedMemory",
                ),
            )
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("storageProvider")
    fun `test glue creation`(fixture: StorageFixture) {
        fixture.use {
            val glue = Glue(it.storage)
            assertNotNull(glue, "Glue instance should be created")
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("storageProvider")
    fun `test basic query`(fixture: StorageFixture) =
        runBlocking {
            fixture.use {
                val glue = Glue(it.storage)

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
        }

    @ParameterizedTest(name = "{0}")
    @MethodSource("storageProvider")
    fun `test sql value types`(fixture: StorageFixture) =
        runBlocking {
            fixture.use {
                val glue = Glue(it.storage)

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
        }

    @ParameterizedTest(name = "{0}")
    @MethodSource("storageProvider")
    fun `test error handling - non-existent table`(fixture: StorageFixture) {
        fixture.use {
            val glue = Glue(it.storage)

            // Query non-existent table should throw exception
            assertThrows(GlueSqlException::class.java) {
                runBlocking {
                    glue.query("SELECT * FROM non_existent_table")
                }
            }
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("storageProvider")
    fun `test error handling - invalid sql`(fixture: StorageFixture) {
        fixture.use {
            val glue = Glue(it.storage)

            // Invalid SQL should throw exception
            assertThrows(GlueSqlException::class.java) {
                runBlocking {
                    glue.query("INVALID SQL SYNTAX")
                }
            }
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("storageProvider")
    fun `test multiple queries`(fixture: StorageFixture) =
        runBlocking {
            fixture.use {
                val glue = Glue(it.storage)

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
}
