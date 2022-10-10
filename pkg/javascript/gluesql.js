import init, { Glue } from './dist/web/gluesql_js.js';

let loaded = false;

async function load() {
  await init();

  loaded = true;
}

export async function gluesql() {
  if (!loaded) {
    await load();
  }

  return new Glue();
}
