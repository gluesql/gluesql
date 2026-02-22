# GlueSQL Kotlin

GlueSQL Kotlin is a Kotlin binding for the [GlueSQL](https://github.com/gluesql/gluesql) database engine. It provides an embedded SQL database for JVM applications with a selection of storage backends.

Supported storages:

- `Storage.Memory`
- `Storage.Json`
- `Storage.Sled`
- `Storage.SharedMemory`

Learn more at **<https://gluesql.org/docs>**.

## Installation

### Gradle
```kotlin
dependencies {
    implementation("org.gluesql:gluesql-kotlin:0.1.0")
}
```

### Maven
```xml
<dependency>
    <groupId>org.gluesql</groupId>
    <artifactId>gluesql-kotlin</artifactId>
    <version>0.1.0</version>
</dependency>
```

## Usage

```kotlin
import org.gluesql.core.Glue
import org.gluesql.core.Storage
import kotlinx.coroutines.runBlocking

fun main() = runBlocking {
    val glue = Glue(Storage.Memory)

    glue.query(
        """
        CREATE TABLE User (id INTEGER, name TEXT);
        INSERT INTO User VALUES (1, 'Hello'), (2, 'World');
        """
    )

    val results = glue.query("SELECT * FROM User;")
    println(results)
}
```

## License

This project is licensed under the Apache License, Version 2.0 - see the [LICENSE](https://raw.githubusercontent.com/gluesql/gluesql/main/LICENSE) file for details.
