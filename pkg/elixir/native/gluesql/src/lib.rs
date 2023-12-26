mod glue;
mod payload;
mod result;
mod storages;

use {
    glue::glue_new,
    rustler::{Env, Term},
    storages::memory_storage::{memory_storage_new, ExMemoryStorageResource},
};

fn on_load(env: Env, _info: Term) -> bool {
    rustler::resource!(ExMemoryStorageResource, env);
    true
}

rustler::init!(
    "Elixir.GlueSQL.Native",
    [glue_new, memory_storage_new],
    load = on_load
);
