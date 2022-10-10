import wasm from "./Cargo.toml";

let Glue;

export async function gluesql() {
  if (!Glue) {
    Glue = (await wasm()).Glue;
  }

  return new Glue();
}
