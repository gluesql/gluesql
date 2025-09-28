import { expect, it } from "vitest";
// @ts-expect-error
const { Glue }: typeof import('../../index.js') = await import('../../browser.js')

it("Builded wasm package should loaded", async () => {
  const glue = new Glue();
  expect(glue).toBeDefined();
});
