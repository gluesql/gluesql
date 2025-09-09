package org.gluesql

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.gluesql.uniffi.Glue
import org.gluesql.uniffi.QueryResult
import org.gluesql.uniffi.GlueSqlException
import org.gluesql.uniffi.Storage
import kotlin.coroutines.CoroutineContext

/**
 * Coroutine-based client for GlueSQL operations.
 * Suitable for Kotlin applications that use coroutines for asynchronous programming.
 */
class CoroutineGlueSQLClient(
    storage: Storage,
    private val context: CoroutineContext = Dispatchers.IO
) {
    private val glue = Glue(storage)

    /**
     * Execute SQL query asynchronously using coroutines.
     * 
     * @param sql SQL query string
     * @return List of query results
     * @throws GlueSqlException if query execution fails
     */
    suspend fun query(sql: String): List<QueryResult> {
        return withContext(context) {
            glue.query(sql)
        }
    }
}
