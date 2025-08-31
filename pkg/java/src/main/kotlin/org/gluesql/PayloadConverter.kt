package org.gluesql

import com.google.gson.Gson
import com.google.gson.reflect.TypeToken

internal class PayloadConverter {
    private val gson = Gson()
    
    fun convertJsonResults(jsonResults: List<String>): List<QueryResult> {
        return jsonResults.map { jsonString ->
            val payloadType = object : TypeToken<Map<String, Any>>() {}.type
            val payloadData = gson.fromJson<Map<String, Any>>(jsonString, payloadType)
            QueryResult.fromPayload(payloadData)
        }
    }
}
