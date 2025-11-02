import org.gradle.api.Project
import org.gradle.kotlin.dsl.register
import java.io.File

/**
 * Registers tasks for managing native libraries in resources.
 * These tasks are primarily used for creating distribution JARs with
 * pre-built native libraries for multiple platforms.
 *
 * @param workspaceRoot Root directory of the Cargo workspace
 */
fun Project.registerNativeLibraryTasks(workspaceRoot: File) {
    registerCopyNativeLibsTask(workspaceRoot)
    registerCleanNativeLibsTask()
}

/**
 * Registers the `copyNativeLibs` task that copies pre-built native libraries
 * from the Cargo target directory to the resources directory.
 *
 * This is typically used by CI to create a fat JAR with libraries for all platforms.
 */
private fun Project.registerCopyNativeLibsTask(workspaceRoot: File) {
    tasks.register("copyNativeLibs") {
        group = "build"
        description = "Copy pre-built native libraries to resources (for distribution)"

        val resourcesDir = projectDir.resolve("src/main/resources/natives")

        doLast {
            // Clean and recreate natives directory
            project.delete(resourcesDir)
            resourcesDir.mkdirs()

            var copiedCount = 0
            platforms.forEach { platform ->
                val sourceFile = workspaceRoot.resolve(
                    "target/${platform.rustTarget}/release/${platform.libFileName}"
                )
                val destDir = resourcesDir.resolve(platform.jnaPrefix)

                if (sourceFile.exists()) {
                    copy {
                        from(sourceFile)
                        into(destDir)
                    }
                    copiedCount++
                    logger.lifecycle("✓ Copied ${platform.jnaPrefix}/${platform.libFileName}")
                } else {
                    logger.warn("⚠ Native library not found: ${sourceFile.absolutePath}")
                    logger.warn("  This is expected if you haven't built for ${platform.rustTarget}")
                }
            }

            if (copiedCount == 0) {
                logger.warn("⚠ No native libraries were copied!")
                logger.warn("  Run 'cargo build --release --target <TARGET>' for each platform first,")
                logger.warn("  or let CI build them for you.")
            } else {
                logger.lifecycle("✓ Copied $copiedCount/${platforms.size} platform libraries")
            }
        }
    }
}

/**
 * Registers the `cleanNativeLibs` task that removes native libraries from the resources directory.
 * This is useful for cleaning up before a fresh build.
 */
private fun Project.registerCleanNativeLibsTask() {
    tasks.register("cleanNativeLibs") {
        group = "build"
        description = "Remove native libraries from resources"

        doLast {
            project.delete(projectDir.resolve("src/main/resources/natives"))
            logger.lifecycle("✓ Cleaned native libraries from resources")
        }
    }
}
