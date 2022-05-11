import rust from "@wasm-tool/rollup-plugin-rust";

export default {
    input: "gluesql.js",
    output: {
      dir: "../dist/bundler",
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
