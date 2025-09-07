package org.gluesql

import kotlinx.coroutines.*
import kotlinx.coroutines.future.future
import org.gluesql.uniffi.*

class GlueSQL(storage: Storage) {
    private val glue = Glue(storage)

    suspend fun query(sql: String): List<QueryResult> = glue.query(sql)

    @OptIn(DelicateCoroutinesApi::class)
    fun queryFuture(sql: String) = GlobalScope.future { query(sql) }

    fun queryBlocking(sql: String) = runBlocking { query(sql) }
}
