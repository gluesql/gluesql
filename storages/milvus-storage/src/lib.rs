mod error;
mod store;
mod utils;

use {
    error::ResultExt,
    gluesql_core::error::Result,
    milvus::{
        client::{Client, ClientBuilder},
    },
};

pub struct MilvusStorageConfig {
    url: String,
    username: Option<String>,
    password: Option<String>,
    db_name: Option<String>,
}

impl MilvusStorageConfig {
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            username: None,
            password: None,
            db_name: None,
        }
    }

    pub fn with_basic_auth(
        mut self,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        self.username = Some(username.into());
        self.password = Some(password.into());
        self
    }

    pub fn with_db_name(mut self, db_name: impl Into<String>) -> Self {
        self.db_name = Some(db_name.into());
        self
    }

    fn into_parts(self) -> (String, Option<String>, Option<String>, Option<String>) {
        (self.url, self.username, self.password, self.db_name)
    }
}

pub struct MilvusStorage {
    client: Client,
    db_name: Option<String>,
}

impl MilvusStorage {
    pub async fn new(config: MilvusStorageConfig) -> Result<Self> {
        let (url, username, password, db_name) = config.into_parts();

        let mut builder = ClientBuilder::new(url);

        if let Some(username) = username.as_ref() {
            builder = builder.username(username);

            if let Some(password) = password.as_ref() {
                builder = builder.password(password);
            }
        }

        let client = builder.build().await.map_storage_err()?;

        Ok(
            Self {
                client,
                db_name,
            }
        )
    }
}
