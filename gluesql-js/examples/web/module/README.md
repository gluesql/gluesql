## Guide: Running module example

### How to run?
0. Before going on, you should build and generate `dist` directory. So go to under `gluesql/gluesql-js/web`. And run these commands
```sh
# install dependencies
$ yarn

# build for examples/web/module
$ yarn build:browser
```

1. Go to under `gluesql/gluesql-js` 

2. Serve files using proper application
There are many http server applications. Here are some examples

- [simple-http-server](https://crates.io/crates/simple-http-server)
```sh
# 1. install
$ cargo install simple-http-server

# 2. serve files under `gluesql/gluesql-js`. Now open the browser and go to `http://localhost:3030`
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
