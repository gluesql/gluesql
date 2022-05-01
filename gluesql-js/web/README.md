## 🚴 Usage

### Setup
Enable `opt-level = "s"` option
```
# ref. https://github.com/rustwasm/wasm-pack/issues/1111
# enable this only for gluesql-js build
[profile.release]
opt-level = "s"
```

### Build (rollup)
```
yarn rollup -c
```

### Build (browser module & webpack)
```
wasm-pack build --target web --no-typescript --release --out-dir ../dist/web
```

### Build (nodejs)
```
wasm-pack build --target nodejs --no-typescript --release --out-dir ../dist/nodejs
```

### 🔬 Test in Headless Browsers with `wasm-pack test`

```
wasm-pack test --headless --firefox --chrome
```
