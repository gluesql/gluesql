import org.gradle.api.Project

/**
 * Main entry point for registering all native build-related Gradle tasks.
 *
 * This function registers local development tasks:
 * - buildRustLib: Build Rust library in debug mode
 * - generateBindings: Generate UniFFI Kotlin bindings
 *
 * Call this from build.gradle.kts to set up the native build pipeline.
 */
fun Project.registerNativeBuildTasks() {
    val workspaceRoot = projectDir.parentFile.parentFile
    registerRustBuildTasks(workspaceRoot)
}
