package org.gluesql

import kotlinx.coroutines.runBlocking
import org.gluesql.uniffi.Glue
import org.gluesql.uniffi.QueryResult
import org.gluesql.uniffi.GlueSqlException
import org.gluesql.uniffi.Storage

/**
 * Blocking synchronous client for GlueSQL operations.
 * Suitable for traditional Java applications that prefer blocking I/O.
 */
class GlueSQLClient(storage: Storage) {
    private val glue = Glue(storage)

    /**
     * Execute SQL query synchronously (blocking).
     * 
     * @param sql SQL query string
     * @return List of query results
     * @throws GlueSqlException if query execution fails
     */
    @Throws(GlueSqlException::class)
    fun query(sql: String): List<QueryResult> {
        return runBlocking { glue.query(sql) }
    }
}
