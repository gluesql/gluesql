import { expect, it } from "vitest";
import { Glue } from "../../gluesql-napi.wasi-browser";

it("Builded wasm package should loaded", async () => {
  const glue = new Glue();
  expect(glue).toBeDefined();
});
