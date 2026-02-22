import org.gradle.api.GradleException
import org.gradle.internal.os.OperatingSystem

/**
 * Returns the native library name for the current platform.
 * @return Library name with or without 'lib' prefix depending on OS
 */
fun currentPlatformLibName(): String =
    when {
        OperatingSystem.current().isWindows -> "gluesql_kotlin"
        else -> "libgluesql_kotlin"
    }

/**
 * Returns the file extension for native libraries on the current platform.
 * @return "so" for Linux, "dylib" for macOS, "dll" for Windows
 * @throws GradleException if the platform is not supported
 */
fun currentPlatformLibExtension(): String =
    when {
        OperatingSystem.current().isLinux -> "so"
        OperatingSystem.current().isMacOsX -> "dylib"
        OperatingSystem.current().isWindows -> "dll"
        else -> throw GradleException("Unsupported operating system: ${OperatingSystem.current()}")
    }
