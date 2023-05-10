import rust from "@wasm-tool/rollup-plugin-rust";
      // dir: "../dist/bundler",

export default {
    input: "gluesql.js",
    output: {
      format: "es",
      sourcemap: true,
      name: 'gluesql',
    },
    plugins: [
      rust({
        inlineWasm: true,
      }),
    ],
};
