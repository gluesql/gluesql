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
# all together
yarn run build

# rollup
yarn run build:rollup

# browser module & webpack
yarn run build:browser

# nodejs
yarn run build:nodejs
```

### 🔬 Test in Headless Browsers with `wasm-pack test`
```
wasm-pack test --headless --firefox --chrome
```
