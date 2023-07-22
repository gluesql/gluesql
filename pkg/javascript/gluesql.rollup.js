import init, { Glue } from './dist_web/gluesql_js.js';
import loadDB from './dist_web/gluesql_js_bg.wasm';

let loaded = false;

export async function gluesql() {
  if (!loaded) {
    const instance = await loadDB();
    await init(instance);

    loaded = true;
  }

  return new Glue();
}
