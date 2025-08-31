package org.gluesql

sealed class QueryResult {
    data class Create(val rows: Long) : QueryResult()
    data class Insert(val rows: Long) : QueryResult()
    data class Update(val rows: Long) : QueryResult()
    data class Delete(val rows: Long) : QueryResult()
    data class Select(val rows: List<List<String>>, val labels: List<String>) : QueryResult()
    data class DropTable(val count: Long) : QueryResult()
    object AlterTable : QueryResult()
    object StartTransaction : QueryResult()
    object Commit : QueryResult()
    object Rollback : QueryResult()
    data class ShowVariable(val name: String, val value: String) : QueryResult()
    data class ShowColumns(val columns: List<String>) : QueryResult()
    
    companion object {
        fun fromPayload(data: Map<String, Any>): QueryResult {
            return when {
                data.containsKey("Create") -> {
                    val createData = data["Create"] as Map<String, Any>
                    Create((createData["rows"] as Double).toLong())
                }
                data.containsKey("Insert") -> {
                    val insertData = data["Insert"] as Map<String, Any>
                    Insert((insertData["rows"] as Double).toLong())
                }
                data.containsKey("Update") -> {
                    val updateData = data["Update"] as Map<String, Any>
                    Update((updateData["rows"] as Double).toLong())
                }
                data.containsKey("Delete") -> {
                    val deleteData = data["Delete"] as Map<String, Any>
                    Delete((deleteData["rows"] as Double).toLong())
                }
                data.containsKey("Select") -> {
                    val selectData = data["Select"] as Map<String, Any>
                    @Suppress("UNCHECKED_CAST")
                    val rows = selectData["rows"] as List<List<String>>
                    @Suppress("UNCHECKED_CAST")
                    val labels = selectData["labels"] as List<String>
                    Select(rows, labels)
                }
                data.containsKey("DropTable") -> {
                    val dropData = data["DropTable"] as Map<String, Any>
                    DropTable((dropData["count"] as Double).toLong())
                }
                data.containsKey("AlterTable") -> AlterTable
                data.containsKey("StartTransaction") -> StartTransaction
                data.containsKey("Commit") -> Commit
                data.containsKey("Rollback") -> Rollback
                data.containsKey("ShowVariable") -> {
                    val showData = data["ShowVariable"] as Map<String, Any>
                    ShowVariable(showData["name"] as String, showData["value"] as String)
                }
                data.containsKey("ShowColumns") -> {
                    val columnsData = data["ShowColumns"] as Map<String, Any>
                    @Suppress("UNCHECKED_CAST")
                    ShowColumns(columnsData["columns"] as List<String>)
                }
                else -> throw IllegalArgumentException("Unknown payload type: $data")
            }
        }
    }
}
