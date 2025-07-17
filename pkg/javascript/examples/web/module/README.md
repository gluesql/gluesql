## Guide: Running module example

### How to run?
0. Before going on, you should build and generate `dist_web` directory. So go to `pkg/javascript` and run this command:
```sh
wasm-pack build --no-pack --target web --no-typescript --release --out-dir ./dist_web
```

1. Go to under `pkg/javascript`

2. Serve files using proper application
There are many http server applications. Here are some examples

- [simple-http-server](https://crates.io/crates/simple-http-server)
```sh
# 1. install
$ cargo install simple-http-server

# 2. serve files under `pkg/javascript`. Now open the browser and go to `http://localhost:3000`
$ simple-http-server --port 3000

# 3. navigate to `examples/web/module/index.html`. The url should be `http://localhost:3000/examples/web/module/index.html`
# 4. Check the result
```

- [http-server](https://www.npmjs.com/package/http-server)
```sh
# 1. install or run
$ npx http-server

# Remaining steps are same as `simple-http-server` section guide
```
