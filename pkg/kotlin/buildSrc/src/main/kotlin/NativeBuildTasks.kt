import org.gradle.api.Project

/**
 * Main entry point for registering all native build-related Gradle tasks.
 *
 * This function registers:
 * - Local development tasks: buildRustLib, generateBindings
 * - CI-specific tasks: organizeNativeLibs
 *
 * Call this from build.gradle.kts to set up the native build pipeline.
 */
fun Project.registerNativeBuildTasks() {
    val workspaceRoot = projectDir.parentFile.parentFile
    registerRustBuildTasks(workspaceRoot)
    registerCITasks()
}
