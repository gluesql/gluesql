#![cfg(feature = "test-redis")]

use {
    gluesql_redis_storage::pool::ConnectionPool,
    redis::Commands,
    std::{env, fs, sync::Arc, thread, time::Duration},
};

fn test_client() -> redis::Client {
    let mut path = env::current_dir().unwrap();
    path.push("tests/redis-storage.toml");
    let redis_config_str = fs::read_to_string(path).unwrap();
    let redis_config: toml::Value = toml::from_str(&redis_config_str).unwrap();
    let url = redis_config["redis"]["url"].as_str().unwrap();
    let port: u16 = redis_config["redis"]["port"].as_integer().unwrap() as u16;

    redis::Client::open(format!("redis://{url}:{port}")).unwrap()
}

#[test]
fn checkout_allows_concurrent_access_up_to_pool_size() {
    let client = test_client();
    let pool = ConnectionPool::new(&client, 2).unwrap();

    let mut conn_a = pool.checkout().unwrap();
    let mut conn_b = pool.checkout().unwrap();

    let _: () = conn_a.set("pool_test_a", "1").unwrap();
    let _: () = conn_b.set("pool_test_b", "2").unwrap();

    let val_a: String = conn_a.get("pool_test_a").unwrap();
    let val_b: String = conn_b.get("pool_test_b").unwrap();
    assert_eq!(val_a, "1");
    assert_eq!(val_b, "2");
}

#[test]
fn checkout_blocks_until_a_connection_is_returned() {
    let client = test_client();
    let pool = Arc::new(ConnectionPool::new(&client, 1).unwrap());

    let conn = pool.checkout().unwrap();

    let pool2 = Arc::clone(&pool);
    let handle = thread::spawn(move || {
        let _conn2 = pool2.checkout().unwrap();
    });

    thread::sleep(Duration::from_millis(200));
    assert!(!handle.is_finished(), "checkout should still be blocked");

    drop(conn);
    handle.join().unwrap();
}
