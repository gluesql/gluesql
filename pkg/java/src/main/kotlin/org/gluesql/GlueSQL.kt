package org.gluesql

import kotlinx.coroutines.*
import uniffi.gluesql.*

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

    fun queryBlocking(sql: String): List<QueryResult> = runBlocking {
        query(sql)
    }
}
