mod glue;
mod storages;

use {
    glue::glue_new,
    rustler::{Env, Term},
    storages::memory_storage::{memory_storage_new, ExMemoryStorageRef},
};

fn on_load(env: Env, _info: Term) -> bool {
    rustler::resource!(ExMemoryStorageRef, env);
    true
}

rustler::init!(
    "Elixir.GlueSQL.Native",
    [glue_new, memory_storage_new],
    load = on_load
);
