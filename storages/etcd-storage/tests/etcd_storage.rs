#![cfg(feature = "test-etcd")]

use {
    async_trait::async_trait, gluesql_core::prelude::Glue, gluesql_etcd_storage::EtcdStorage,
    test_suite::*,
};

struct EtcdTester {
    glue: Glue<EtcdStorage>,
}

#[async_trait(?Send)]
impl Tester<EtcdStorage> for EtcdTester {
    async fn new(namespace: &str) -> Self {
        let endpoints = ["localhost:2379"];
        let mut storage = EtcdStorage::new(&endpoints, namespace).await.expect("etcd");
        // clean existing data
        let prefix = format!("{}/", namespace);
        let _ = storage
            .client
            .kv_client()
            .delete(
                prefix.clone(),
                Some(etcd_client::DeleteOptions::new().with_prefix()),
            )
            .await;
        let glue = Glue::new(storage);

        EtcdTester { glue }
    }

    fn get_glue(&mut self) -> &mut Glue<EtcdStorage> {
        &mut self.glue
    }
}

generate_store_tests!(tokio::test, EtcdTester);
generate_alter_table_tests!(tokio::test, EtcdTester);
