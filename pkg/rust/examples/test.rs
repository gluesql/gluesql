use anyhow::{Result, anyhow};
use gluesql::prelude::{Glue, MemoryStorage};

fn main() {
    futures::executor::block_on(run()).expect("hey");
}

async fn run() -> Result<()> {
    let mem = MemoryStorage::default();
    let mut mem = Glue::new(mem);

    mem.execute(
        r#"
            CREATE TABLE IF NOT EXISTS posts (
                id UUID PRIMARY KEY,
                title TEXT NOT NULL,
                text TEXT NOT NULL,
                id_2 UUID NOT NULL
            )
        "#,
    )
    .await?;

    let uuid = uuid::Uuid::now_v7();
    let temp = mem
        .execute(format!(
            r#"INSERT INTO "posts" ("id", "title", "text", "id_2") VALUES ('{uuid}', 'Homo', 'いいよ、来いよ', '{uuid}')"#
        ))
        .await?;
    println!("DEBUG insert: {:#?}", temp);

    let temp = mem.execute("SELECT * from posts").await?;
    println!("DEBUG query all: {:#?}", temp);
    let temp = temp[0]
        .select()
        .ok_or(anyhow!("No result"))?
        .next()
        .ok_or(anyhow!("No next"))?;
    let temp = temp
        .get("id")
        .cloned()
        .ok_or(anyhow!("Cannot get id"))?
        .clone();
    let uuid_2 = match temp {
        gluesql::prelude::Value::Uuid(val) => uuid::Uuid::from_u128(val),
        _ => todo!(),
    };
    assert_eq!(uuid, uuid_2);
    println!("DEBUG query first uuid: {:?}", uuid_2);

    let temp = format!(
        "SELECT * from posts where id = CAST({} AS UUID)",
        uuid_2.as_u128()
    );
    println!("DEBUG SQL: {temp}");
    let temp = mem.execute(temp).await?;
    println!("DEBUG sub query: {:?}", temp);

    let temp = format!(
        "SELECT * from posts where id = UUID '{}'",
        uuid_2.to_string()
    );
    println!("DEBUG SQL: {temp}");
    let temp = mem.execute(temp).await?;
    println!("DEBUG sub query: {:?}", temp);

    let temp = format!("SELECT * from posts where text = '{}'", "いいよ、来いよ");
    println!("DEBUG SQL: {temp}");
    let temp = mem.execute(temp).await?;
    println!("DEBUG sub query: {:?}", temp);

    let temp = format!("SELECT * from posts where id_2 = '{}'", uuid_2.to_string());
    println!("DEBUG SQL: {temp}");
    let temp = mem.execute(temp).await?;
    println!("DEBUG sub query: {:?}", temp);

    Ok(())
}
