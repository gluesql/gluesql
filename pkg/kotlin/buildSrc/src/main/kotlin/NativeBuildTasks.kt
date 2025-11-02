import org.gradle.api.Project

/**
 * Main entry point for registering all native build-related Gradle tasks.
 *
 * This function registers:
 * - Rust build tasks (buildRustLib, generateBindings)
 * - Native library management tasks (copyNativeLibs, cleanNativeLibs)
 *
 * Call this from build.gradle.kts to set up the native build pipeline.
 */
fun Project.registerNativeBuildTasks() {
    val workspaceRoot = projectDir.parentFile.parentFile

    registerRustBuildTasks(workspaceRoot)
    registerNativeLibraryTasks(workspaceRoot)
}
