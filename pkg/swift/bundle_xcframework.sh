rm -rf ./Gluesql.xcframework && mv ./out_swift/gluesqlFFI.modulemap ./out_swift/module.modulemap &&
xcodebuild -create-xcframework \
  -library ../../target/aarch64-apple-ios/release/libgluesql.a -headers ./out_swift \
  -library ../../target/aarch64-apple-ios-sim/release/libgluesql.a -headers ./out_swift \
  -output Gluesql.xcframework
