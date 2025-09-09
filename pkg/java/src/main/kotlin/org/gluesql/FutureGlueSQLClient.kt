package org.gluesql

import kotlinx.coroutines.DelicateCoroutinesApi
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.future.future
import org.gluesql.uniffi.Glue
import org.gluesql.uniffi.QueryResult
import org.gluesql.uniffi.GlueSqlException
import org.gluesql.uniffi.Storage
import java.util.concurrent.CompletableFuture

/**
 * Asynchronous client for GlueSQL operations using CompletableFuture.
 * Suitable for modern Java applications that prefer non-blocking I/O.
 */
class FutureGlueSQLClient(storage: Storage) {
    private val glue = Glue(storage)

    /**
     * Execute SQL query asynchronously.
     * 
     * @param sql SQL query string
     * @return CompletableFuture with query results
     * @throws GlueSqlException if query execution fails
     */
    @OptIn(DelicateCoroutinesApi::class)
    @Throws(GlueSqlException::class)
    fun query(sql: String): CompletableFuture<List<QueryResult>> {
        return GlobalScope.future { glue.query(sql) }
    }
}
