package org.gluesql.org.gluesql

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*
import kotlinx.coroutines.runBlocking
import uniffi.gluesql.Storage

class GlueSQLTest {
    private fun createGlue() = GlueSQL(Storage.Memory)

    @Test
    fun testCreateTable() = runBlocking {
        val glue = createGlue()
        val sql = "CREATE TABLE test (id INTEGER, name TEXT)"
        val results = glue.query(sql)
        
        assertEquals(1, results.size)
        assertTrue(results[0] is QueryResult.Create)
        assertEquals(0, (results[0] as QueryResult.Create).rows)
    }

    @Test
    fun testInsertAndSelect() = runBlocking {
        val glue = createGlue()
        glue.query("CREATE TABLE users (id INTEGER, name TEXT)")
        
        val insertResult = glue.query("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')")
        assertEquals(1, insertResult.size)
        assertTrue(insertResult[0] is QueryResult.Insert)
        assertEquals(2, (insertResult[0] as QueryResult.Insert).rows)
        
        val selectResult = glue.query("SELECT * FROM users ORDER BY id")
        assertEquals(1, selectResult.size)
        assertTrue(selectResult[0] is QueryResult.Select)
        
        val selectData = selectResult[0] as QueryResult.Select
        assertEquals(listOf("id", "name"), selectData.labels)
        assertEquals(2, selectData.rows.size)
        assertEquals(listOf("1", "Alice"), selectData.rows[0])
        assertEquals(listOf("2", "Bob"), selectData.rows[1])
    }

    @Test
    fun testUpdate() = runBlocking {
        val glue = createGlue()
        glue.query("CREATE TABLE users (id INTEGER, name TEXT)")
        glue.query("INSERT INTO users VALUES (1, 'Alice')")
        
        val updateResult = glue.query("UPDATE users SET name = 'Alice Smith' WHERE id = 1")
        assertEquals(1, updateResult.size)
        assertTrue(updateResult[0] is QueryResult.Update)
        assertEquals(1, (updateResult[0] as QueryResult.Update).rows)
        
        val selectResult = glue.query("SELECT name FROM users WHERE id = 1")
        val selectData = selectResult[0] as QueryResult.Select
        assertEquals("Alice Smith", selectData.rows[0][0])
    }

    @Test
    fun testDelete() = runBlocking {
        val glue = createGlue()
        glue.query("CREATE TABLE users (id INTEGER, name TEXT)")
        glue.query("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')")
        
        val deleteResult = glue.query("DELETE FROM users WHERE id = 1")
        assertEquals(1, deleteResult.size)
        assertTrue(deleteResult[0] is QueryResult.Delete)
        assertEquals(1, (deleteResult[0] as QueryResult.Delete).rows)
        
        val selectResult = glue.query("SELECT COUNT(*) FROM users")
        val selectData = selectResult[0] as QueryResult.Select
        assertEquals("1", selectData.rows[0][0])
    }

    @Test
    fun testDropTable() = runBlocking {
        val glue = createGlue()
        glue.query("CREATE TABLE test_table (id INTEGER)")
        
        val dropResult = glue.query("DROP TABLE test_table")
        assertEquals(1, dropResult.size)
        assertTrue(dropResult[0] is QueryResult.DropTable)
        assertEquals(1L, (dropResult[0] as QueryResult.DropTable).count)
    }

    @Test
    fun testInvalidSQL() = runBlocking {
        val glue = createGlue()
        assertThrows(GlueSQLException::class.java) {
            runBlocking {
                glue.query("INVALID SQL STATEMENT")
            }
        }
    }

    @Test
    fun testJsonStorage() = runBlocking {
        val jsonFile = "/tmp/test_${System.currentTimeMillis()}.json"
        val jsonGlue = GlueSQL(Storage.Json(jsonFile))
        
        val results = jsonGlue.query("CREATE TABLE test_json (id INTEGER)")
        assertEquals(1, results.size)
        assertTrue(results[0] is QueryResult.Create)
    }
}
