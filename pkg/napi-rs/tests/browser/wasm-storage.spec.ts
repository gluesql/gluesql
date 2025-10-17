import { expect, it } from "vitest";
import { Glue } from "../../gluesql-napi.wasi-browser";

it("Glue constructs and supports setting memory engine", async () => {
  const glue = new Glue();

  expect(glue).toBeDefined();

  // ensure setting default engine to memory works (synchronous call)
  expect(() => glue.setDefaultEngine('memory')).not.toThrow();
});

it("Glue supports setting localStorage engine in browser", async () => {
  const glue = new Glue();

  // set storage to localStorage (available in browser environment)
  expect(() => glue.setDefaultEngine('localStorage')).not.toThrow();
});
