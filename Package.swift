// swift-tools-version:5.7
import PackageDescription

let package = Package(
    name: "Gluesql", // The name of your package
    platforms: [
        .iOS(.v18),
        // .macOS(.v11)
    ],
    products: [
        .library(
            name: "Gluesql",
            targets: ["Gluesql"]
        ),
    ],
    targets: [
        // 1. YOUR SWIFT CODE TARGET
        // Contains the generated .swift file and your manual extensions.
        .target(
            name: "Gluesql",
            dependencies: ["gluesqlFFI"], // Depends on the binary target
            path: "pkg/swift/Gluesql/Sources/Gluesql"
        ),

        // 2. YOUR BINARY XCFRAMEWORK TARGET
        // This points to the compiled Rust library.
        .binaryTarget(
            name: "gluesqlFFI", // This is the internal FFI module name
            path: "pkg/swift/Gluesql/Artifacts/Gluesql.xcframework"
        )
    ]
)
