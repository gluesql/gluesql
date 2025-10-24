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

## Quick Start

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

## Storage Backends

GlueSQL supports multiple storage backends:

- **Memory** - In-memory storage (data lost on restart)
  ```kotlin
  StorageFactory.memory()
  ```

- **JSON File** - Simple file-based storage
  ```kotlin
  StorageFactory.json("/path/to/database.json")
  ```

- **Sled** - Embedded key-value database
  ```kotlin
  val config = SledConfigBuilder.create("/path/to/db")
      .cacheCapacity(4096L)
      .mode(Mode.HIGH_THROUGHPUT)
      .build()
  StorageFactory.sled(config)
  ```

- **Shared Memory** - Shared memory storage
  ```kotlin
  StorageFactory.sharedMemory()
  ```

## Platform Support

The JAR includes native libraries for all major platforms:
- Linux (x86-64)
- macOS (x86-64 Intel)
- macOS (ARM64 Apple Silicon)
- Windows (x86-64)

**The correct native library is automatically loaded at runtime** - no configuration needed!

## Development

See [DEVELOPMENT.md](DEVELOPMENT.md) for:
- Setting up the development environment
- Building from source
- Running tests
- Code formatting guidelines
- Contributing guidelines

## How It Works

GlueSQL Java bindings use:
- **Rust** for the core database engine (high performance, memory safe)
- **UniFFI** to generate Kotlin/Java bindings automatically
- **JNA** to load native libraries at runtime

The distribution JAR is a "Fat JAR" containing native libraries for all platforms. At runtime, JNA detects your platform and automatically extracts and loads the appropriate library.

For more details, see [NATIVE_LOADING.md](NATIVE_LOADING.md).

## License

This project is licensed under the Apache License, Version 2.0 - see the [LICENSE](https://raw.githubusercontent.com/gluesql/gluesql/main/LICENSE) file for details.
