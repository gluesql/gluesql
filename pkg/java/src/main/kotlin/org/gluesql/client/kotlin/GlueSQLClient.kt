package org.gluesql.client.kotlin

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.gluesql.uniffi.Glue
import org.gluesql.uniffi.QueryResult
import org.gluesql.uniffi.Storage
import kotlin.coroutines.CoroutineContext

/**
 * GlueSQL client optimized for Kotlin applications.
 * 
 * This client provides native Kotlin coroutine support for asynchronous database operations.
 * Use this client when working in Kotlin environments where you want to leverage
 * structured concurrency and suspend functions.
 * 
 * ## Usage Example
 * 
 * ```kotlin
 * // In a suspend function or coroutine scope
 * val client = GlueSQLClient(storage)
 * val results = client.query("SELECT * FROM users")
 * 
 * // With custom coroutine context
 * val client = GlueSQLClient(storage, Dispatchers.Default)
 * val results = client.query("SELECT * FROM posts WHERE user_id = ?", userId)
 * ```
 * 
 * ## Coroutine Context
 * 
 * By default, queries are executed on [Dispatchers.IO] which is optimized for
 * I/O operations. You can provide a custom [CoroutineContext] if needed.
 * 
 * @param storage The storage backend to use for database operations
 * @param context The coroutine context for query execution (defaults to Dispatchers.IO)
 * @constructor Creates a new GlueSQL client instance for Kotlin applications
 */
class GlueSQLClient(
    storage: Storage,
    private val context: CoroutineContext = Dispatchers.IO
) {
    private val glue = Glue(storage)

    /**
     * Execute SQL query asynchronously using coroutines.
     * 
     * This suspend function integrates seamlessly with Kotlin's structured concurrency.
     * It will suspend the current coroutine until the query completes, allowing other
     * coroutines to run in the meantime.
     *
     * @param sql SQL query string to execute
     * @return List of query results
     * @throws org.gluesql.uniffi.GlueSqlException if query execution fails
     */
    suspend fun query(sql: String): List<QueryResult> {
        return withContext(context) {
            glue.query(sql)
        }
    }
}
