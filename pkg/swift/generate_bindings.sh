cargo build --release &&
cargo run --bin uniffi-bindgen generate --library ../../target/release/libgluesql.dylib --language swift --out-dir ./out_swift
