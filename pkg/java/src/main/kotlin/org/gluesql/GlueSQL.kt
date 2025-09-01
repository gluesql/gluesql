package org.gluesql

import kotlinx.coroutines.*
import kotlinx.coroutines.future.future
import org.gluesql.uniffi.*

class GlueSQL(storage: Storage) {
    private val glue = try {
        Glue(storage)
    } catch (e: GlueSqlException) {
        throw GlueSQLException("Failed to create GlueSQL instance: ${e.message}", e)
    }
    
    private val payloadConverter = PayloadConverter()
    
    suspend fun query(sql: String): List<QueryResult> {
        try {
            val results = glue.query(sql)
            return payloadConverter.convertJsonResults(results)
        } catch (e: GlueSqlException) {
            throw GlueSQLException(e.message ?: "Unknown GlueSQL error", e)
        }
    }

    @OptIn(DelicateCoroutinesApi::class)
    fun queryFuture(sql: String) = GlobalScope.future { query(sql) }

    fun queryBlocking(sql: String) = runBlocking { query(sql) }
}
