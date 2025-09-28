package org.gluesql.client.java

import org.gluesql.uniffi.SledConfig
import org.gluesql.uniffi.Storage

/**
 * Factory class for creating Storage instances with Java-friendly syntax.
 * 
 * This class provides static methods that Java developers can use to create
 * storage instances without dealing with Kotlin-specific syntax.
 * 
 * ## Usage Examples
 * 
 * ```java
 * // Memory storage
 * Storage storage = StorageFactory.memory();
 * 
 * // JSON file storage
 * Storage storage = StorageFactory.json("/path/to/database.json");
 * 
 * // Sled storage
 * Storage storage = StorageFactory.sled("/path/to/database");
 * 
 * // Shared memory storage
 * Storage storage = StorageFactory.sharedMemory("my_namespace");
 * ```
 */
class StorageFactory {
    companion object {
        @JvmStatic
        fun memory(): Storage = Storage.Memory

        @JvmStatic
        fun json(path: String): Storage = Storage.Json(path)
        
        @JvmStatic
        @JvmOverloads
        fun sled(path: String, config: SledConfig? = null): Storage = Storage.Sled(path, config)

        @JvmStatic
        fun sharedMemory(namespace: String): Storage = Storage.SharedMemory(namespace)
    }
}
