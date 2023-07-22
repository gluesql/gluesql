## 🚴 Usage

### Setup
Enable `opt-level = "s"` option
```
# ref. https://github.com/rustwasm/wasm-pack/issues/1111
# enable this only for gluesql-js build
[profile.release]
opt-level = "s"
```

### Build
```
# browser module, webpack and rollup
wasm-pack build --no-pack --target web --no-typescript --release --out-dir ./dist_web

# nodejs
wasm-pack build --no-pack --target nodejs --no-typescript --release --out-dir ./dist_nodejs -- --no-default-features --features nodejs
```

### 🔬 Test in Headless Browsers with `wasm-pack test`
```
wasm-pack test --headless --firefox --chrome
```
