---
sidebar_position: 5
---

# Kotlin

This guide will help you get started with GlueSQL in a Kotlin project. First, add the `gluesql` dependency to your project.

## Installation

### Gradle

Add the following dependencies to your `build.gradle.kts` file:

```kotlin
dependencies {
    implementation("org.gluesql:gluesql:0.1.0")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.7.3")
}
```

### Maven

Add the following dependencies to your `pom.xml` file:

```xml
<dependencies>
    <dependency>
        <groupId>org.gluesql</groupId>
        <artifactId>gluesql</artifactId>
        <version>0.1.0</version>
    </dependency>
    <dependency>
        <groupId>org.jetbrains.kotlinx</groupId>
        <artifactId>kotlinx-coroutines-core</artifactId>
        <version>1.7.3</version>
    </dependency>
</dependencies>
```

## Usage

Here's a simple example of using GlueSQL in a Kotlin application:

```kotlin
import org.gluesql.client.kotlin.GlueSQLClient
import org.gluesql.storage.StorageFactory
import org.gluesql.uniffi.QueryResult
import kotlinx.coroutines.runBlocking

fun main() = runBlocking {
    // Create GlueSQL client with memory storage
    val client = GlueSQLClient(StorageFactory.memory())
    
    // Create table and insert data
    client.query("CREATE TABLE User (id INTEGER, name TEXT)")
    client.query("INSERT INTO User VALUES (1, 'glue'), (2, 'sql')")
    
    // Query data
    val results = client.query("SELECT * FROM User")
    val selectResult = results[0] as QueryResult.Select
    
    println("Columns: ${selectResult.result.labels}")
    println("Rows: ${selectResult.result.rows}")
}
```

## Storage Types

GlueSQL supports multiple storage backends:

### Memory Storage
```kotlin
val client = GlueSQLClient(StorageFactory.memory())
```

### JSON File Storage
```kotlin
val client = GlueSQLClient(StorageFactory.json("/path/to/database.json"))
```

### Sled Storage
```kotlin
import org.gluesql.storage.config.SledConfigBuilder

val config = SledConfigBuilder.create("/path/to/database").build()
val client = GlueSQLClient(StorageFactory.sled(config))
```

### Shared Memory Storage
```kotlin
val client = GlueSQLClient(StorageFactory.sharedMemory())
```

## Coroutines

Kotlin coroutines provide excellent support for asynchronous database operations:

```kotlin
import kotlinx.coroutines.*

suspend fun performDatabaseOperations() {
    val client = GlueSQLClient(StorageFactory.memory())
    
    // Sequential operations
    client.query("CREATE TABLE products (id INTEGER, name TEXT, price REAL)")
    client.query("INSERT INTO products VALUES (1, 'Laptop', 999.99)")
    
    // Concurrent operations
    val deferredResults = listOf(
        async { client.query("CREATE TABLE orders (id INTEGER, total REAL)") },
        async { client.query("CREATE TABLE customers (id INTEGER, name TEXT)") }
    )
    
    deferredResults.awaitAll()
    println("All tables created")
}
```

## Error Handling

Handle database errors with try-catch blocks:

```kotlin
import org.gluesql.uniffi.GlueSqlException

try {
    client.query("SELECT * FROM non_existent_table")
} catch (e: GlueSqlException) {
    println("Database error: ${e.message}")
}
```

This example demonstrates the basic usage of GlueSQL in Kotlin applications with coroutine support, table creation, data insertion, and querying.