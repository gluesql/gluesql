mod storages;

use {
    rustler::{Env, Term},
    storages::memory_storage::{ExMemoryStorage, ExMemoryStorageRef},
};

#[rustler::nif]
fn glue_memory_storage() -> ExMemoryStorage {
    ExMemoryStorage::new()
}

fn on_load(env: Env, _info: Term) -> bool {
    rustler::resource!(ExMemoryStorageRef, env);
    true
}

rustler::init!(
    "Elixir.GlueSQL.Native",
    [glue_memory_storage],
    load = on_load
);
