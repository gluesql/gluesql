package org.gluesql

import kotlinx.coroutines.DelicateCoroutinesApi
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.future.future
import kotlinx.coroutines.runBlocking
import org.gluesql.uniffi.Glue

@OptIn(DelicateCoroutinesApi::class)
fun Glue.queryFuture(sql: String) = GlobalScope.future { query(sql) }
fun Glue.queryBlocking(sql: String) = runBlocking { query(sql) }
