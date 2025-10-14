cargo build --release --target aarch64-apple-ios && cargo build --release --target aarch64-apple-ios-sim && \
cargo build --release && \
cargo run --bin uniffi-bindgen generate --library ../../target/release/libgluesql.dylib --language swift --out-dir ./out_swift &&  \
rm -rf ./Gluesql/Artifacts/Gluesql.xcframework && mv ./out_swift/gluesqlFFI.modulemap ./out_swift/module.modulemap && \
xcodebuild -create-xcframework \
  -library ../../target/aarch64-apple-ios/release/libgluesql.a -headers ./out_swift \
  -library ../../target/aarch64-apple-ios-sim/release/libgluesql.a -headers ./out_swift \
  -output ./Gluesql/Artifacts/Gluesql.xcframework
