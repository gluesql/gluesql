# GlueSQL Java/Kotlin Bindings

Java and Kotlin bindings for GlueSQL using UniFFI.

## Requirements

- Rust 1.70+
- Java 11+
- Kotlin 1.9+
- Gradle 8.0+

## Build

```bash
# Build Rust library and generate bindings
./gradlew build

# Run tests
./gradlew test
```

## Usage

### Kotlin

```kotlin
import org.gluesql.*
import kotlinx.coroutines.runBlocking

fun main() = runBlocking {
    // Create GlueSQL instance with memory storage
    val glue = GlueSQLWrapper(Storage.Memory)
    
    // Create table
    glue.query("CREATE TABLE users (id INTEGER, name TEXT)")
    
    // Insert data
    glue.query("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')")
    
    // Query data
    val results = glue.query("SELECT * FROM users")
    val selectResult = results[0] as QueryResult.Select
    
    println("Labels: ${selectResult.labels}")
    selectResult.rows.forEach { row ->
        println("Row: $row")
    }
}
```

### Java

```java
import org.gluesql.*;
import java.util.List;

public class Example {
    public static void main(String[] args) {
        GlueSQLWrapper glue = new GlueSQLWrapper(Storage.Memory.INSTANCE);
        
        // Use blocking API for Java
        List<QueryResult> results = glue.queryBlocking("CREATE TABLE test (id INTEGER)");
        System.out.println("Table created");
        
        glue.queryBlocking("INSERT INTO test VALUES (1)");
        List<QueryResult> selectResults = glue.queryBlocking("SELECT * FROM test");
        
        QueryResult.Select selectResult = (QueryResult.Select) selectResults.get(0);
        System.out.println("Data: " + selectResult.getRows());
    }
}
```

## Storage Types

- `Storage.Memory` - In-memory storage
- `Storage.Json(path)` - JSON file storage
- `Storage.Sled(path, config)` - Sled database storage
- `Storage.SharedMemory(namespace)` - Shared memory storage

## Error Handling

All GlueSQL errors are wrapped in `GlueSQLException` with the original error details.

```kotlin
try {
    glue.query("INVALID SQL")
} catch (e: GlueSQLException) {
    println("Error: ${e.message}")
}
```