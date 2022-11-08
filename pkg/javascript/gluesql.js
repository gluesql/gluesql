import init, { Glue } from './dist/web/gluesql_js.js';

let loaded = false;

async function load(module_or_path) {
  await init(module_or_path);

  loaded = true;
}

export async function gluesql(module_or_path) {
  if (!loaded) {
    await load(module_or_path);
  }

  return new Glue();
}
