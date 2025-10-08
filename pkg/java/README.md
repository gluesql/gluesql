# GlueSQL Java/Kotlin

GlueSQL Java/Kotlin bindings provide a SQL database engine for JVM applications. It works as an embedded database and supports multiple storage backends including memory, JSON file, Sled, and shared memory.

Learn more at **<https://gluesql.org/docs>**

* [Getting Started - Java](https://gluesql.org/docs/dev/getting-started/java)
* [Getting Started - Kotlin](https://gluesql.org/docs/dev/getting-started/kotlin)
* [SQL Syntax](https://gluesql.org/docs/dev/sql-syntax/intro)

## Installation

### Gradle
```kotlin
dependencies {
    implementation("org.gluesql:gluesql:0.1.0")
}
```

### Maven
```xml
<dependency>
    <groupId>org.gluesql</groupId>
    <artifactId>gluesql</artifactId>
    <version>0.1.0</version>
</dependency>
```

## Usage

### Java

```java
import org.gluesql.client.java.GlueSQLClient;
import org.gluesql.storage.StorageFactory;
import org.gluesql.uniffi.QueryResult;

public class Example {
    public static void main(String[] args) throws Exception {
        GlueSQLClient client = new GlueSQLClient(StorageFactory.memory());
        
        client.query("CREATE TABLE User (id INTEGER, name TEXT)");
        client.query("INSERT INTO User VALUES (1, 'Hello'), (2, 'World')");
        
        List<QueryResult> results = client.query("SELECT * FROM User");
        QueryResult.Select selectResult = (QueryResult.Select) results.get(0);
        
        System.out.println(selectResult.getResult().getRows());
    }
}
```

### Kotlin

```kotlin
import org.gluesql.client.kotlin.GlueSQLClient
import org.gluesql.storage.StorageFactory
import kotlinx.coroutines.runBlocking

fun main() = runBlocking {
    val client = GlueSQLClient(StorageFactory.memory())
    
    client.query("CREATE TABLE User (id INTEGER, name TEXT)")
    client.query("INSERT INTO User VALUES (1, 'Hello'), (2, 'World')")
    
    val results = client.query("SELECT * FROM User")
    val selectResult = results[0] as QueryResult.Select
    
    println(selectResult.result.rows)
}
```

## Storage Types

- `StorageFactory.memory()` - In-memory storage
- `StorageFactory.json(path)` - JSON file storage
- `StorageFactory.sled(config)` - Sled database storage
- `StorageFactory.sharedMemory()` - Shared memory storage

## License

This project is licensed under the Apache License, Version 2.0 - see the [LICENSE](https://raw.githubusercontent.com/gluesql/gluesql/main/LICENSE) file for details.