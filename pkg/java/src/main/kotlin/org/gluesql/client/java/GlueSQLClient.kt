package org.gluesql.client.java

import kotlinx.coroutines.DelicateCoroutinesApi
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.future.future
import kotlinx.coroutines.runBlocking
import org.gluesql.uniffi.Glue
import org.gluesql.uniffi.GlueSqlException
import org.gluesql.uniffi.QueryResult
import org.gluesql.uniffi.Storage
import java.util.concurrent.CompletableFuture

/**
 * GlueSQL client optimized for Java applications.
 * 
 * This client provides both synchronous and asynchronous query execution methods
 * tailored for Java development patterns. Use this client when working in pure Java
 * environments or when you prefer Java's concurrency models.
 * 
 * ## Usage Examples
 * 
 * ### Synchronous execution (blocking):
 * ```java
 * GlueSQLClient client = new GlueSQLClient(storage);
 * List<QueryResult> results = client.query("SELECT * FROM users");
 * ```
 * 
 * ### Asynchronous execution (non-blocking):
 * ```java
 * GlueSQLClient client = new GlueSQLClient(storage);
 * CompletableFuture<List<QueryResult>> future = client.queryAsync("SELECT * FROM users");
 * future.thenAccept(results -> {
 *     // Process results
 * });
 * ```
 * 
 * @param storage The storage backend to use for database operations
 * @constructor Creates a new GlueSQL client instance for Java applications
 */
class GlueSQLClient(storage: Storage) {
    private val glue = Glue(storage)

    /**
     * Execute SQL query synchronously (blocking).
     * 
     * This method blocks the current thread until the query completes.
     * Suitable for simple applications or when you need immediate results.
     *
     * @param sql SQL query string to execute
     * @return List of query results
     * @throws GlueSqlException if query execution fails
     */
    @Throws(GlueSqlException::class)
    fun query(sql: String): List<QueryResult> {
        return runBlocking { glue.query(sql) }
    }

    /**
     * Execute SQL query asynchronously (non-blocking).
     * 
     * This method returns immediately with a CompletableFuture that will
     * complete when the query finishes. Suitable for high-performance
     * applications that need to handle multiple concurrent operations.
     *
     * @param sql SQL query string to execute
     * @return CompletableFuture that will complete with query results
     * @throws GlueSqlException if query execution fails
     */
    @OptIn(DelicateCoroutinesApi::class)
    @Throws(GlueSqlException::class)
    fun queryAsync(sql: String): CompletableFuture<List<QueryResult>> {
        return GlobalScope.future { glue.query(sql) }
    }
}
